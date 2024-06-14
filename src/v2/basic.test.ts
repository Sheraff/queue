import assert from "node:assert"
import { test } from 'node:test'
import { Queue, createProgram, step } from "./queue"
import { exhaustQueue } from "./test.utils"

test.describe('benchmark', () => {
	test('synchronous', async (t) => {
		const queue = new Queue({
			hello: createProgram({ id: 'hello' }, async () => {
				for (let i = 0; i < 100; i++) {
					await step.run('a', () => 'a')
				}
			})
		})
		performance.mark('start')
		await queue.registry.hello.invoke()
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`100 sync steps took ${duration.toFixed(2)}ms`)
		assert(duration < 5, `Benchmark took ${duration}ms, expected less than 5ms`)
		await queue.close()
	})
	test('asynchronous', async (t) => {
		const queue = new Queue({
			hello: createProgram({ id: 'hello' }, async () => {
				for (let i = 0; i < 100; i++) {
					await step.run('a', async () => 'a')
				}
			})
		})
		performance.mark('start')
		await queue.registry.hello.invoke()
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`100 async steps took ${duration.toFixed(2)}ms`)
		assert(duration < 50, `Benchmark took ${duration}ms, expected less than 50ms`)
		await queue.close()
	})
})

test('memo', async (t) => {
	let count = 0
	const queue = new Queue({
		hey: createProgram({ id: 'hey' }, async () => {
			await step.run('ya', () => { count++ })
		})
	})
	queue.registry.hey.dispatch()
	queue.registry.hey.dispatch()
	queue.registry.hey.dispatch()
	queue.registry.hey.dispatch()
	await exhaustQueue(queue)
	assert.strictEqual(count, 1, 'Step should have been memoized')
	await queue.close()
})