import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import assert from "node:assert"
import { invoke } from "./utils"
import Database from "better-sqlite3"
import type { Task } from "../lib/storage"

test.describe('benchmark', {
	skip: !!process.env.CI,
	timeout: 5000
}, () => {
	test('synchronous', async (t) => {
		const hello = new Job({
			id: 'hello',
		}, async () => {
			for (let i = 0; i < 10000; i++) {
				await Job.run('a', () => 'a')
			}
		})

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello },
			storage: new SQLiteStorage()
		})

		performance.mark('start')
		await invoke(queue.jobs.hello, {})
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`10000 sync steps took ${duration.toFixed(2)}ms (< 150ms)`)
		t.diagnostic(`Overall: ${(duration / 10).toFixed(2)} µs/step`)
		assert(duration < 150, `Benchmark took ${duration.toFixed(2)}ms, expected less than 150ms`)

		await queue.close()
		performance.clearMarks()
	})

	test('asynchronous', async (t) => {
		const hello = new Job({
			id: 'hello',
		}, async () => {
			for (let i = 0; i < 100; i++) {
				await Job.run('a', async () => 'a')
			}
		})

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello },
			storage: new SQLiteStorage()
		})

		performance.mark('start')
		await invoke(queue.jobs.hello, {})
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`100 async steps took ${duration.toFixed(2)}ms (< 100ms)`)
		t.diagnostic(`Overall: ${(duration * 10).toFixed(2)} µs/step`)
		assert(duration < 100, `Benchmark took ${duration}ms, expected less than 100ms`)

		await queue.close()
		performance.clearMarks()
	})

	test('combinatorics', async (t) => {
		const DEPTH = 7
		const hello = new Job({
			id: 'hello',
		}, async ({
			branch,
			depth
		}: {
			branch: string,
			depth: number
		}): Promise<{ treasure: number }> => {
			if (depth === DEPTH) {
				return { treasure: 1 }
			}
			const results = await Promise.all([
				Job.invoke(hello, { branch: `${branch}-1`, depth: depth + 1 }),
				Job.invoke(hello, { branch: `${branch}-2`, depth: depth + 1 }),
			])
			return { treasure: results.reduce((acc, { treasure }) => acc + treasure, 0) }
		})

		const db = new Database()

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello },
			storage: new SQLiteStorage({ db }),
		})

		let count = 0
		queue.jobs.hello.emitter.on('trigger', () => count++)
		const allDone = new Promise<{ treasure: number } | null>((resolve) => queue.jobs.hello.emitter.on('settled', ({ result }) => {
			count--
			if (count === 0) resolve(result)
		}))

		performance.mark('com-start')
		queue.jobs.hello.dispatch({ branch: 'root', depth: 0 })
		const res = await allDone
		performance.mark('com-end')

		t.diagnostic(`Depth: ${DEPTH}`)

		const duration = performance.measure('hello', 'com-start', 'com-end').duration
		t.diagnostic(`Combinatorics took ${duration.toFixed(2)}ms (~2s for depth 8, ~600ms for depth 7, <200ms for depth 6, <50ms for depth 5)`)

		t.diagnostic(`Combinatorics result: ${res?.treasure}`)
		assert(res?.treasure === (2 ** DEPTH), `Expected ${2 ** (DEPTH)} treasures, got ${res?.treasure}`)

		await queue.close()

		const rows = db.prepare('SELECT * FROM tasks').all() as Task[]
		t.diagnostic(`Tasks count: ${rows.length}`)
		assert(rows.length === (2 ** (DEPTH + 1)) - 1, `Expected ${(2 ** (DEPTH + 1)) - 1} tasks, got ${rows.length}`)

		db.close()
		performance.clearMarks()
	})

	test('many wait for pipe', async (t) => {
		const pipe = new Pipe({
			id: 'pipe',
			in: {} as { num: number },
		})

		const hello = new Job({
			id: 'hello',
		}, async ({ }: { i: number }): Promise<{ res: number }> => {
			const { num } = await Job.waitFor(pipe, {})
			return { res: num }
		})

		const pollution = new Job({
			id: 'pollution',
		}, async ({ }: { i: number }): Promise<{ res: number }> => {
			const { num } = await Job.waitFor(pipe, { filter: { num: -1 } })
			return { res: num }
		})

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello, pollution },
			pipes: { pipe },
			storage: new SQLiteStorage(),
		})

		let count = 0
		queue.jobs.hello.emitter.on('trigger', () => count++)
		const allDone = new Promise<{ res: number } | null>((resolve) => queue.jobs.hello.emitter.on('settled', ({ result }) => {
			count--
			if (count === 0) resolve(result)
		}))

		const COUNT = 250
		for (let i = 0; i < COUNT * 2; i++) {
			if (i % 2 === 0)
				queue.jobs.pollution.dispatch({ i })
			else
				queue.jobs.hello.dispatch({ i })
		}

		await new Promise((resolve) => setTimeout(resolve, 10))

		performance.mark('pipe-start')
		queue.pipes.pipe.dispatch({ num: 42 })
		const res = await allDone
		performance.mark('pipe-end')

		const duration = performance.measure('hello', 'pipe-start', 'pipe-end').duration
		t.diagnostic(`Many wait for pipe took ${duration.toFixed(2)}ms (< 100ms) for ${COUNT} steps with ${COUNT} unrelated tasks in the database`)
		t.diagnostic(`Overall: ${(duration / COUNT).toFixed(2)} ms/step`)

		t.diagnostic(`Many wait for pipe result: ${res?.res}`)
		assert(res?.res === 42, `Expected 42, got ${res?.res}`)

		await queue.close()
	})

	test('many parallel', async (t) => {
		const hello = new Job({
			id: 'hello',
		}, async () => {
			const a = await Job.run('a', async () => 'a')
			return { a }
		})

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello },
			storage: new SQLiteStorage(),
		})

		let count = 0
		queue.jobs.hello.emitter.on('trigger', () => count++)
		const promise = new Promise((resolve) => queue.jobs.hello.emitter.on('success', () => {
			count--
			if (count === 0) resolve(null)
		}))
		performance.mark('start')
		const COUNT = 1000
		for (let k = 0; k < COUNT; k++) {
			queue.jobs.hello.dispatch({ k })
		}
		await promise
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`${COUNT} parallel jobs took ${duration.toFixed(2)}ms (< ${COUNT}ms)`)
		t.diagnostic(`Overall: ${(duration / COUNT * 1000).toFixed(2)} µs/step`)
		assert(duration < COUNT, `Benchmark took ${duration}ms, expected less than ${COUNT}ms`)

		await queue.close()
		performance.clearMarks()
	})
})

