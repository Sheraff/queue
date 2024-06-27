import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import assert from "node:assert"
import { invoke } from "./utils"

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
		t.diagnostic(`10000 sync steps took ${duration.toFixed(2)}ms`)
		t.diagnostic(`Overall: ${(duration / 10000).toFixed(4)} ms/step`)
		assert(duration < 150, `Benchmark took ${duration.toFixed(2)}ms, expected less than 150ms`)

		await queue.close()
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
		t.diagnostic(`100 async steps took ${duration.toFixed(2)}ms`)
		t.diagnostic(`Overall: ${(duration / 100).toFixed(4)} ms/step`)
		assert(duration < 100, `Benchmark took ${duration}ms, expected less than 100ms`)

		await queue.close()
	})
})