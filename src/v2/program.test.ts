import assert from "node:assert"
import { test } from 'node:test'
import { Queue, createProgram, forwardInterrupt, step } from "./queue"
import { z } from "zod"
import { exhaustQueue } from "./test.utils"

test.describe('cancel', () => {
	test('during task', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({ id: 'hello' }, async () => {
				await step.run('a', () => found.push('a'))
				await step.sleep(20)
				await step.run('b', async () => found.push('b'))
				await step.sleep(20)
				await step.run('c', () => found.push('c'))
			})
		})
		setTimeout(() => queue.registry.hello.cancel(), 30)
		await queue.registry.hello.invoke()
		t.diagnostic('Steps executed: ' + found.join(', '))
		assert.strictEqual(found.join(','), 'a,b', 'Only a and b should have been executed')
		await queue.close()
	})
	test('between tasks', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({ id: 'hello' }, async () => {
				await step.run('a', () => found.push('a'))
				await step.sleep(20)
				await step.run('b', async () => {
					found.push('b')
					await new Promise(r => setTimeout(r, 20))
				})
				await step.run('c', () => found.push('c'))
			})
		})
		setTimeout(() => queue.registry.hello.cancel(), 30)
		await queue.registry.hello.invoke()
		t.diagnostic('Steps executed: ' + found.join(', '))
		assert.strictEqual(found.join(','), 'a,b', 'Only a and b should have been executed')
		await queue.close()
	})
})

test.describe('timeout', () => {
	test('during task', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({ id: 'hello', timings: { timeout: 30 } }, async () => {
				await step.run('a', () => found.push('a'))
				await step.sleep(20)
				await step.run('b', async () => found.push('b'))
				await step.sleep(20)
				await step.run('c', () => found.push('c'))
			})
		})
		await queue.registry.hello.invoke()
		t.diagnostic('Steps executed: ' + found.join(', '))
		assert.strictEqual(found.join(','), 'a,b', 'Only a and b should have been executed')
		await queue.close()
	})
	test('between tasks', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({ id: 'hello', timings: { timeout: 30 } }, async () => {
				await step.run('a', () => found.push('a'))
				await step.sleep(20)
				await step.run('b', async () => {
					found.push('b')
					await new Promise(r => setTimeout(r, 20))
				})
				await step.run('c', () => found.push('c'))
			})
		})
		await queue.registry.hello.invoke()
		t.diagnostic('Steps executed: ' + found.join(', '))
		assert.strictEqual(found.join(','), 'a,b', 'Only a and b should have been executed')
		await queue.close()
	})
})

test.describe('priority', () => {
	test('higher priority task should be executed first', async (t) => {
		const order: number[] = []
		let externalResolve: () => void
		const externalEvent = new Promise<void>(r => externalResolve = r)
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				priority: (input) => input.priority,
				input: z.object({ priority: z.number() })
			}, async (input) => {
				await step.run('fake fetch', async () => {
					await externalEvent
				})
				await step.run('log', async () => {
					order.push(input.priority)
				})
			}),
		})
		const before = queue.registry.hello.invoke({ priority: 0 })
		await new Promise(r => setTimeout(r, 10))
		assert.strictEqual(order.length, 0, 'Step should not have been executed yet')
		const after = queue.registry.hello.invoke({ priority: 99 })
		externalResolve!()

		await Promise.all([before, after])
		t.diagnostic('Steps executed: ' + order.join(', '))
		assert.strictEqual(order.join(','), '99,0', 'Higher priority task should be executed first')
		await queue.close()
	})
})

test.describe('debounce', () => {
	test('debounced task should be executed only once', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { debounce: 15 },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(input.key))
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hello.dispatch({ key: 'b' })
		await new Promise(r => setTimeout(r, 10))
		queue.registry.hello.dispatch({ key: 'c' })
		await new Promise(r => setTimeout(r, 30))
		queue.registry.hello.dispatch({ key: 'd' })
		await new Promise(r => queue.emitter.once('system/success', r))
		assert.strictEqual(found.join(','), 'c,d', 'Only the last task should have been executed')
		await queue.close()
	})
	test('debouncing works across multiple programs', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { debounce: { timeout: 10, id: 'group-id' } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(`hello:${input.key}`))
			}),
			hola: createProgram({
				id: 'hola',
				timings: { debounce: { timeout: 10, id: 'group-id' } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(`hola:${input.key}`))
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hola.dispatch({ key: 'b' })
		queue.registry.hello.dispatch({ key: 'c' })
		queue.registry.hola.dispatch({ key: 'd' })
		await new Promise(r => queue.emitter.once('system/success', r))
		assert.strictEqual(found.join(','), 'hola:d', 'Only the last task should have been executed')
		await queue.close()
	})
})

test.describe('throttle', () => {
	test('throttled task should be executed only once', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { throttle: 15 },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(input.key))
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hello.dispatch({ key: 'b' })
		await new Promise(r => setTimeout(r, 30))
		queue.registry.hello.dispatch({ key: 'c' })
		await new Promise(r => setTimeout(r, 10))
		queue.registry.hello.dispatch({ key: 'd' })
		await exhaustQueue(queue)
		assert.strictEqual(found.join(','), 'a,c', 'Only the first task should have been executed')
		await queue.close()
	})
	test('throttling works across multiple programs', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { throttle: { timeout: 10, id: 'group-id' } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(`hello:${input.key}`))
			}),
			hola: createProgram({
				id: 'hola',
				timings: { throttle: { timeout: 10, id: 'group-id' } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('a', () => found.push(`hola:${input.key}`))
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hola.dispatch({ key: 'b' })
		queue.registry.hello.dispatch({ key: 'c' })
		queue.registry.hola.dispatch({ key: 'd' })
		await new Promise(r => queue.emitter.once('system/success', r))
		assert.strictEqual(found.join(','), 'hello:a', 'Only the first task should have been executed')
	})
})

test.describe('concurrency', () => {
	test('concurrent tasks should be executed in parallel', async (t) => {
		const timings: Record<string, { start: number, end: number }> = {}
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { concurrency: 2 },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('before', () => {
					timings[input.key] = { start: Date.now(), end: 0 }
				})
				await step.sleep(10)
				await step.run('after', () => {
					timings[input.key]!.end = Date.now()
				})
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hello.dispatch({ key: 'b' })
		queue.registry.hello.dispatch({ key: 'c' })
		queue.registry.hello.dispatch({ key: 'd' })
		await exhaustQueue(queue)
		const total = timings.d!.end - timings.a!.start
		t.diagnostic(`Total time: ${total}ms (concurrency 2, 4 tasks of 10ms each)`)
		assert(total > 20 && total < 30, 'Batches of to makes 2 runs of 10ms each')
		await queue.close()
	})
	test('concurrency works across multiple programs, with in-between delay', async (t) => {
		const timings: Record<string, { start: number, end: number }> = {}
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				timings: { concurrency: { id: 'group-id', delay: 10, limit: 2 } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('before', () => {
					timings[input.key] = { start: Date.now(), end: 0 }
				})
				await step.sleep(10)
				await step.run('after', () => {
					timings[input.key]!.end = Date.now()
				})
			}),
			hola: createProgram({
				id: 'hola',
				timings: { concurrency: { id: 'group-id', delay: 10, limit: 2 } },
				input: z.object({ key: z.string() })
			}, async (input) => {
				await step.run('before', () => {
					timings[input.key] = { start: Date.now(), end: 0 }
				})
				await step.sleep(10)
				await step.run('after', () => {
					timings[input.key]!.end = Date.now()
				})
			})
		})
		queue.registry.hello.dispatch({ key: 'a' })
		queue.registry.hola.dispatch({ key: 'b' })
		queue.registry.hello.dispatch({ key: 'c' })
		queue.registry.hola.dispatch({ key: 'd' })
		await exhaustQueue(queue)
		const total = timings.d!.end - timings.a!.start
		t.diagnostic(`Total time: ${total}ms (concurrency 2, 4 tasks of 10ms each, 10ms enforced delay)`)
		assert(total > 30 && total < 45, 'Batches of to makes 2 runs of 10ms each, plus 10ms in between')
		await queue.close()
	})
})

test.describe('triggers', () => {
	test('event', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				input: z.object({ key: z.string() }),
				triggers: { event: 'boo' }
			}, async (input) => {
				await step.run('a', () => found.push(input.key))
			}),
			other: createProgram({
				id: 'other',
				input: z.object({ key: z.string() }),
				triggers: { event: ['boo'] }
			}, async (input) => {
				await step.run('a', () => found.push(input.key))
			})
		})
		queue.emitter.emit('boo', { key: 'a' })
		await exhaustQueue(queue)
		assert.strictEqual(found.join(','), 'a,a', 'Both programs should have been executed')
		await queue.close()
	})
	// skipped because it's too long
	test.skip('cron', async (t) => {
		const found: string[] = []
		const queue = new Queue({
			hello: createProgram({
				id: 'hello',
				input: z.object({ date: z.string().datetime() }),
				triggers: { cron: '*/1 * * * * *' }, // min duration for a cron is 1s
			}, async () => {
				await step.run('a', () => found.push('a'))
			})
		})
		await new Promise(r => setTimeout(r, 2100))
		assert.strictEqual(found.length, 2, 'Step should have been executed once')
		await queue.close()
	})
})

// TODO: test queue can be killed and recreated from DB
