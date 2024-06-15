import assert from "node:assert"
import { test } from 'node:test'
import { Queue, createProgram, step } from "./queue"
import { z } from "zod"
import { listenAll } from "./test.utils"

test.describe('benchmark', { skip: !!process.env.CI }, () => {
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

test.describe('memo', () => {
	test('steps do not re-execute', async (t) => {
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
	test('tasks do not re-execute', async (t) => {
		let count = 0
		const queue = new Queue({
			hey: createProgram({
				id: 'hey',
				input: z.object({ id: z.string() }),
				output: z.object({ count: z.number() }),
			}, async () => {
				await step.run('ya', () => { count++ })
				return { count }
			})
		})
		const one = await queue.registry.hey.invoke({ id: 'a' })
		const two = await queue.registry.hey.invoke({ id: 'a' })
		assert.deepEqual(one, { count: 1 }, 'Task should have returned the correct the first time')
		assert.deepEqual(two, { count: 1 }, 'Task should have returned the correct value even when not re-executed')
		assert.notStrictEqual(one, two, 'Task should have returned a new object each time')
		await queue.close()
	})
})

test.describe('events', () => {
	test('success events', async (t) => {
		const events: string[] = []
		const callbacks: string[] = []
		const queue = new Queue({
			hey: createProgram({
				id: 'hey',
				onTrigger: () => callbacks.push('trigger'),
				onStart: () => callbacks.push('start'),
				onSuccess: () => callbacks.push('success'),
				onSettled: () => callbacks.push('settled'),
			}, async () => {
				await step.run('yo', async () => { return 'yo' })
				await step.run('ya', async () => { return 'ya' })
			})
		})
		listenAll(queue, (event) => events.push(event))
		await queue.registry.hey.invoke()
		t.diagnostic(`Events: ${callbacks.join(', ')}`)
		assert.deepEqual(callbacks, ['trigger', 'start', 'success', 'settled'], 'Callbacks should have been triggered in order')
		assert.deepEqual(events, [
			'program/hey/trigger',
			'system/trigger',
			'program/hey/start',
			'system/start',
			'program/hey/continue',
			'system/continue',
			'program/hey/continue',
			'system/continue',
			'program/hey/success',
			'program/hey/settled',
			'system/settled',
			'system/success'
		], 'Events should have been triggered in order')
		await queue.close()
	})
	test('cancel / timeout events', async (t) => {
		const events: string[] = []
		const callbacks: string[] = []
		const queue = new Queue({
			hey: createProgram({
				id: 'hey',
				timings: { timeout: 5 },
				onTrigger: () => callbacks.push('trigger'),
				onStart: () => callbacks.push('start'),
				onCancel: () => callbacks.push('cancel'),
				onTimeout: () => callbacks.push('timeout'),
				onSettled: () => callbacks.push('settled'),
			}, async () => {
				await step.run('yo', async () => { return 'yo' })
				await step.sleep(10)
				await step.run('ya', async () => { return 'ya' })
			})
		})
		listenAll(queue, (event) => events.push(event))
		await queue.registry.hey.invoke()
		t.diagnostic(`Events: ${callbacks.join(', ')}`)
		assert.deepEqual(callbacks, ['trigger', 'start', 'timeout', 'cancel', 'settled'], 'Callbacks should have been triggered in order')
		assert.deepEqual(events, [
			'program/hey/trigger',
			'system/trigger',
			'program/hey/start',
			'system/start',
			'program/hey/continue',
			'system/continue',
			'program/hey/continue',
			'system/continue',
			'program/hey/cancel',
			'program/hey/settled',
			'system/settled',
			'system/cancel'
		], 'Events should have been triggered in order')
		await queue.close()
	})
	test('retry / error events', async (t) => {
		// TODO: don't know yet where "retry" event should be triggered: on a "step" retry? Or a full "program" retry?
		const events: string[] = []
		const callbacks: string[] = []
		const queue = new Queue({
			hey: createProgram({
				id: 'hey',
				onTrigger: () => callbacks.push('trigger'),
				onStart: () => callbacks.push('start'),
				onRetry: () => callbacks.push('retry'), // TODO: not implemented yet
				onError: () => callbacks.push('error'),
				onSettled: () => callbacks.push('settled'),
			}, async () => {
				await step.run({ name: 'yo', retry: { attempts: 2, delay: 1 } }, async () => {
					await new Promise(r => setTimeout(r, 2))
					throw new Error('yo')
				})
			})
		})
		listenAll(queue, (event) => events.push(event))
		await queue.registry.hey.invoke().catch(() => { })
		t.diagnostic(`Events: ${callbacks.join(', ')}`)
		assert.deepEqual(callbacks, [
			'trigger',
			'start',
			// 'retry',
			'error',
			'settled'
		], 'Callbacks should have been triggered in order')
		assert.deepEqual(events, [
			'program/hey/trigger',
			'system/trigger',
			'program/hey/start',
			'system/start',
			'program/hey/continue',
			'system/continue',
			// 'program/hey/retry',
			// 'system/retry',
			'program/hey/continue',
			'system/continue',
			'program/hey/error',
			'program/hey/settled',
			'system/settled',
			'system/error'
		], 'Events should have been triggered in order')
		await queue.close()
	})
})