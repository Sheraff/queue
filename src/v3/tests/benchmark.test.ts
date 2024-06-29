import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import assert from "node:assert"
import { invoke } from "./utils"
import Database from "better-sqlite3"
import type { Task } from "../lib/storage"

test.describe('benchmark', {
	skip: !!process.env.CI,
	timeout: 1000
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
		const hello = new Job({
			id: 'hello',
		}, async ({
			branch,
			depth
		}: {
			branch: string,
			depth: number
		}): Promise<{ treasure: number }> => {
			if (depth === 6) {
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

		queue.jobs.hello.dispatch({ branch: 'root', depth: 0 })

		const res = await allDone
		t.diagnostic(`Combinatorics result: ${res?.treasure}`)
		assert(res?.treasure === 64, `Expected 63 treasures, got ${res?.treasure}`)

		await queue.close()

		const rows = db.prepare('SELECT * FROM tasks').all() as Task[]
		t.diagnostic(`Tasks count: ${rows.length}`)
		assert(rows.length === 127, `Expected 127 tasks, got ${rows.length}`)

		db.close()

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

		const queue = new Queue({
			id: 'benchmark',
			jobs: { hello },
			pipes: { pipe },
			storage: new SQLiteStorage(),
		})

		let count = 0
		queue.jobs.hello.emitter.on('trigger', () => count++)
		const allDone = new Promise<{ res: number } | null>((resolve) => queue.jobs.hello.emitter.on('settled', ({ result }) => {
			count--
			if (count === 0) resolve(result)
		}))

		for (let i = 0; i < 100; i++) {
			queue.jobs.hello.dispatch({ i })
		}

		await new Promise((resolve) => setTimeout(resolve, 10))

		queue.pipes.pipe.dispatch({ num: 42 })

		const res = await allDone
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
		Array.from({ length: 100 }, (_, k) => queue.jobs.hello.dispatch({ k })) // TODO: 1000 jobs take 15s, this is not normal !!!
		await promise
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`100 parallel jobs took ${duration.toFixed(2)}ms (< 100ms)`)
		t.diagnostic(`Overall: ${(duration * 10).toFixed(2)} µs/step`)
		assert(duration < 100, `Benchmark took ${duration}ms, expected less than 100ms`)

		await queue.close()
		performance.clearMarks()
	})
})

