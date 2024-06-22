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
					bbb: string
					bb2: number
					bb3: boolean
				}
			}
			c: { cc: string }[]
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

		path<In>('b.bb.bb3') // $ExpectType "b.bb.bb3"
		path<In>('c[#-1].cc') // $ExpectType "c[#-1].cc"
		// @ts-expect-error
		path<In>('c.cc') // $ExpectError
		// @ts-expect-error
		path<In>('b.bb.bb4') // $ExpectError
		// @ts-expect-error
		path<In>('b.bb.bb3.') // $ExpectError

	})
})