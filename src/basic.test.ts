import assert from "node:assert"
import { test } from 'node:test'
import { Queue, createProgram, step } from "./queue"
import { z } from "zod"

test.describe('benchmark', () => {
	test('synchronous', async (t) => {
		const queue = new Queue({
			hello: createProgram({ id: 'hello' }, async () => {
				for (let i = 0; i < 10000; i++) {
					await step.run('a', () => 'a')
				}
			})
		})
		performance.mark('start')
		await queue.registry.hello.invoke()
		performance.mark('end')
		const duration = performance.measure('hello', 'start', 'end').duration
		t.diagnostic(`10000 sync steps took ${duration.toFixed(2)}ms`)
		assert(duration < 100, `Benchmark took ${duration}ms, expected less than 100ms`)
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
		hey: createProgram({
			id: 'hey',
			input: z.object({ id: z.string() }),
			output: z.object({ id: z.string() }),
		}, async (input) => {
			return step.run('ya', () => { count++; return input })
		})
	})
	queue.registry.hey.dispatch({ id: 'a' })
	queue.registry.hey.dispatch({ id: 'a' })
	queue.registry.hey.dispatch({ id: 'a' })
	queue.registry.hey.dispatch({ id: 'b' })
	queue.registry.hey.dispatch({ id: 'b' })
	const res = await queue.registry.hey.invoke({ id: 'b' })
	assert.strictEqual(count, 2, 'Step should have been memoized')
	assert.deepEqual(res, { id: 'b' }, 'Step should have returned the correct value even when not re-invoked')
	await queue.close()
})