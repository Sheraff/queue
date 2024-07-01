import test from "node:test"
import Database from 'better-sqlite3'
import assert from "node:assert"


test.describe('suite', () => {
	let db: Database.Database
	test.before(() => {
		db = new Database()
		db.pragma('journal_mode = WAL')
	})
	test.after(() => {
		db.close()
	})
	test('SQL JSON filtering', async (t) => {
		db.exec(/* sql */ `
			CREATE TABLE some_table (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				data JSON
			);
		`)

		const insert = db.prepare(/* sql */ `
			INSERT INTO some_table (data) VALUES (@data);
		`)

		insert.run({ data: JSON.stringify({ a: 1, b: { bb: 2 }, c: [3, 4, 5] }) })
		insert.run({ data: JSON.stringify({ a: 99, b: { bb: 3 }, c: [7, 8, 9] }) })
		insert.run({ data: JSON.stringify({ a: 42, b: { bb: 8 }, c: [11, 22] }) })
		insert.run({ data: JSON.stringify({ a: 808, b: null, c: null }) })
		insert.run({ data: JSON.stringify({ a: 504 }) })


		/**
		 * For each row in table `some_table`,
		 * if *every* row of the `json_tree(@filter)` table is a match for the `data` key of the row in `some_table`,
		 * then return that row.
		 * 
		 * A match is defined by
		 * - if the `type` is `'object'`, then the `fullKey` must be a key in the row, also of the type `'object'`
		 * - if the `type` is `'array'`, then the `fullKey` must be a key in the row, also of the type `'array'`
		 * - if the `type` is `'null'`, then ignore it
		 * - for any other `type`, the `value` must be equal to the value in the row
		 * 
		 * Example of `json_tree(@filter)` table, where `filter` is `{"b": {"bb": 2}, "c": null}`:
		 * - key: null, value: '{"b":{"bb":2},"c":null}', type: 'object', atom: null, id: 0, parent: null, fullkey: '$', path: '$'
		 * - key: 'b', value: '{"bb":2}', type: 'object', atom: null, id: 1, parent: 0, fullkey: '$.b', path: '$'
		 * - key: 'bb', value: 2, type: 'integer', atom: 2, id: 4, parent: 1, fullkey: '$.b.bb', path: '$.b'
		 * - key: 'c', value: null, type: 'null', atom: null, id: 9, parent: 0, fullkey: '$.c', path: '$'
		 */
		const filter = db.prepare<{ filter: string }>(/* sql */ `
			WITH filter AS ( -- parse the filter JSON into a table, this could also be inlined in the FROM filter clause below for the same perf (i think?)
				SELECT *
				FROM json_tree(@filter)
				WHERE type != 'null'
			)
			SELECT * FROM some_table as row
			WHERE NOT EXISTS (
				SELECT 1
				FROM filter
				WHERE (
					filter.type = 'object'
					AND (
						json_extract(row.data, filter.fullKey) IS NULL
						OR json_type(json_extract(row.data, filter.fullKey)) != 'object'
					)
				) OR (
					filter.type = 'array'
					AND (
						json_extract(row.data, filter.fullKey) IS NULL
						OR json_type(json_extract(row.data, filter.fullKey)) != 'array'
					)
				) OR (
					filter.type NOT IN ('object', 'array')
					AND (
						json_extract(row.data, filter.fullKey) IS NULL
						OR json_extract(row.data, filter.fullKey) != filter.value
					)
				)
				LIMIT 1 -- short-circuit, if any of the conditions doesn't match, then the row is not a match
			)
		`)

		function getWithFilter(value: unknown) {
			return filter.all({ filter: JSON.stringify(value) })
		}

		// simplest case
		assert.deepEqual(
			getWithFilter({ a: 99 }),
			[{ id: 2, data: '{"a":99,"b":{"bb":3},"c":[7,8,9]}' }]
		)

		// with nesting, ignore null values
		assert.deepEqual(
			getWithFilter({ b: { bb: 3 }, c: null }),
			[{ id: 2, data: '{"a":99,"b":{"bb":3},"c":[7,8,9]}' }]
		)

		// match array item
		assert.deepEqual(
			getWithFilter({ c: [7] }),
			[{ id: 2, data: '{"a":99,"b":{"bb":3},"c":[7,8,9]}' }]
		)

		// match array item at specific index
		assert.deepEqual(
			getWithFilter({ c: [, 8] }),
			[{ id: 2, data: '{"a":99,"b":{"bb":3},"c":[7,8,9]}' }]
		)

		// partial match for nested object
		assert.deepEqual(
			getWithFilter({ a: 1, b: {} }),
			[{ id: 1, data: '{"a":1,"b":{"bb":2},"c":[3,4,5]}' }]
		)

		// no match
		assert.deepEqual(
			getWithFilter({ a: 1, b: { no: 2 } }),
			[]
		)

		// exact match
		assert.deepEqual(
			getWithFilter({ a: 1, b: { bb: 2 }, c: [3, 4, 5] }),
			[{ id: 1, data: '{"a":1,"b":{"bb":2},"c":[3,4,5]}' }]
		)

		// array overflow does not match
		assert.deepEqual(
			getWithFilter({ c: [3, 4, 5, 6] }),
			[]
		)
	})

	test('Object type to JSON path', (t) => {

		type In = {
			a: number
			b: {
				bb: {
					bb1: string
					bb2: number
					bb3: boolean
				}
			}
			c: { cc: string }[]
			d?:
			| string
			| { dd: number }
			| string[]
			e: null
			f: undefined
		}

		type ObjectSubPath<In, K extends keyof In & string = keyof In & string> = {
			[k in K]: `${k}${Path<In[k], '.'>}`
		}[K]

		type Path<In, Prefix extends string = ''> = '' | (
			In extends Array<infer T>
			? `[${number | `#-${number}`}]${Path<T, '.'>}`
			: In extends object
			? `${Prefix}${ObjectSubPath<In>}`
			: ''
		)

		function path<In extends {}>(str: Path<In>) {
			return str
		}

		path<In>('') // accepts empty string
		path<In>('a') // accepts top-level key
		path<In>('b.bb') // accepts nested key
		path<In>('b.bb.bb3') // accepts deep nested key
		path<In>('c[#-1].cc') // accepts array index
		path<In>('d.dd') // accepts optional key
		path<In>('d[0]') // accepts optional array index
		path<In>('e') // accepts null
		path<In>('f') // accepts undefined
		// @ts-expect-error
		path<In>('c.cc') // does not accept non-existant key on array
		// @ts-expect-error
		path<In>('b.bb.bb4') // does not accept non-existant key
		// @ts-expect-error
		path<In>('a[0]') // does not accept array index on non-array
		// @ts-expect-error
		path<In>('b.bb.bb3.') // does not accept trailing dot
	})

	test('Proxy generates JSON path', (t) => {
		type In = {
			a: number
			b: {
				bb: {
					bb1: string
					bb2: number
					bb3: boolean
				}
			}
			c: { cc: string }[]
			d?:
			| string
			| { dd: number }
			| string[]
			e: null
			f: undefined
		}
		const token = Symbol('path')
		const handler: ProxyHandler<{ path: string, loop: boolean }> = {
			get(target, key) {
				const current = target.path
				if (key === token) {
					return current
				}
				if (target.loop || typeof key === 'symbol') {
					throw new Error('Selectors must not contain logic: (OK) data => data.a, (NOT OK) data => data.a > 0 ? data.b : data.c')
				}
				target.loop = true
				if (!current) {
					return new Proxy({ path: key }, handler as any)
				}
				const asNumber = Number(key)
				const path = isNaN(asNumber) ? `.${key}` : asNumber < 0 ? `[#-${-key}]` : `[${key}]`
				return new Proxy({ path: `${current}${path}` }, handler as any)
			},
		}

		function path<In extends object>(select: (obj: In) => any) {
			const proxy = new Proxy<In>({} as In, handler as any)
			const result = select(proxy)
			return result[token]
		}

		// Works for all valid paths

		assert.equal(path<In>(({ a }) => a), 'a')
		assert.equal(path<In>(data => data.b.bb), 'b.bb')
		assert.equal(path<In>(data => data.b.bb.bb3), 'b.bb.bb3')
		assert.equal(path<In>(data => data.c[-1]!.cc), 'c[#-1].cc')
		assert.equal(path<In>(data => data.e), 'e')
		assert.equal(path<In>(data => data.f), 'f')
		// @ts-expect-error -- edge-case: not very handy for when type is a weird union (string | object)
		assert.equal(path<In>(data => data.d!.dd), 'd.dd')
		// @ts-expect-error -- edge-case: not very handy for when type is a weird union (string | Array)
		assert.equal(path<In>(data => data.d![0]), 'd[0]')

		// Issues type errors for invalid paths

		// @ts-expect-error
		path<In>(data => data.c.cc)
		// @ts-expect-error
		path<In>(data => data.b.bb.bb4)
		// @ts-expect-error
		path<In>(data => data.a[0])

		// Throws error for selectors with logic

		assert.throws(() => path<In>(data => data.a > 0 ? data.b : data.c), 'Throws on toPrimitive symbol')
		assert.throws(() => path<In>(data => data.a ? data.b : data.c), 'Throws on multiple accesses to the same object')
		// some forms of logic cannot be detected (ESLint rule could be used to enforce this)
		assert.doesNotThrow(() => path<In>(data => {
			const b = data.b
			if (b) return b.bb
			else return (b as any).bb2
		}), 'Does not throw on simple access')
	})
})

import { AsyncLocalStorage } from "node:async_hooks"
import EventEmitter from "node:events"
test('async local storage through event emitters', (t) => {
	const storage = new AsyncLocalStorage<string>()

	const emitter = new EventEmitter<{ test: [number] }>()

	emitter.on('test', (data) => {
		const store = storage.getStore()
		t.diagnostic(`Store: ${store} => ${data}`)
	})

	emitter.emit('test', 42)

	storage.run('hello', () => {
		emitter.emit('test', 42)
	})
})