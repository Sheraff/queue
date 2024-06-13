import assert from "node:assert"
import { test } from 'node:test'
import { foo } from "./userland/foo.js"
import { Queue, createProgram, forwardInterrupt, step } from "./queue.js"
import { pokemon } from "./userland/pokemon.js"
import { z } from "zod"

test('foo', async (t) => {
	const queue = new Queue({
		foo
	})
	let successes = 0
	queue.emitter.on('program/foo/success', () => successes++)
	const result = await queue.registry.foo.invoke({ fa: '1' })
	assert.strictEqual(result.fi, '1')
	assert.strictEqual(successes, 1)
	await queue.close()
})

test('pokemon', async (t) => {
	const queue = new Queue({
		pokemon,
		foo,
	})
	let successes = 0
	queue.emitter.on('program/pokemon/success', (input) => {
		if (input.id === 25) {
			successes++
		}
	})
	const result = await queue.registry.pokemon.invoke({ id: 25 })
	assert.strictEqual(result.name, 'pikachu')
	await exhaustQueue(queue)
	assert.strictEqual(successes, 1, 'Success event for pokemon#25 should trigger only once')
	await queue.close()
})

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
		assert(duration < 2, `Benchmark took ${duration}ms, expected less than 1ms`)
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

test('sleep', async (t) => {
	const queue = new Queue({
		hey: createProgram({ id: 'hey' }, async () => {
			await step.sleep(100)
		})
	})
	let started = 0
	let ended = 0
	queue.emitter.on('program/hey/start', () => {
		started = Date.now()
	})
	queue.emitter.on('program/hey/success', () => {
		ended = Date.now()
	})
	await queue.registry.hey.invoke()
	assert.notEqual(started, 0, 'Start event should have been triggered')
	assert.notEqual(ended, 0, 'Success event should have been triggered')
	assert(ended - started >= 100, `Sleep should take at least 100ms, took ${ended - started}ms`)
	t.diagnostic(`Sleep took ${ended - started}ms (requested 100ms)`)
	await queue.close()
})

test.describe('retry', () => {
	test('retries and succeeds', async (t) => {
		let successAttempts = 0
		const queue = new Queue({
			eventualSuccess: createProgram({ id: 'eventualSuccess' }, async () => {
				await step.run({ name: 'yo', retry: { attempts: 20 } }, async () => {
					successAttempts++
					if (successAttempts < 3) throw new Error('eventualSuccess userland error')
					return 'passed yo step ok'
				})
				await step.run('ya', () => {
					return 'hello ya step ok'
				})
			}),
		})
		await queue.registry.eventualSuccess.invoke()
		assert.strictEqual(successAttempts, 3, 'Retry should have been attempted 3 times')
		await queue.close()
	})
	test('retries and fails', async (t) => {
		let failAttempts = 0
		const queue = new Queue({
			alwaysFail: createProgram({ id: 'alwaysFail' }, async () => {
				await step.run('yo', async () => {
					await new Promise(r => setTimeout(r, 10))
					failAttempts++
					throw new Error('alwaysFail userland error')
				})
			}),
		})
		await assert.rejects(
			() => queue.registry.alwaysFail.invoke(),
			(err) => {
				assert(err instanceof Error)
				assert(err.cause instanceof Error)
				assert.strictEqual(err.cause.message, 'alwaysFail userland error')
				return true
			},
		)
		assert.strictEqual(failAttempts, 3, 'Retry should have been attempted 3 times, the default')
		await queue.close()
	})
	test('errors can be caught in userland', async (t) => {
		let afterStep = false
		let inCatch = false
		const queue = new Queue({
			catchable: createProgram({ id: 'catchable' }, async () => {
				try {
					await step.run('yo', () => {
						throw new Error('alwaysFail userland error')
					})
					afterStep = true
				} catch (err) {
					forwardInterrupt(err)
					inCatch = true
				}
			})
		})

		await assert.doesNotReject(() => queue.registry.catchable.invoke())
		assert.strictEqual(afterStep, false, 'Step should not have been executed')
		assert.strictEqual(inCatch, true, 'Error should have been caught in userland')
		await queue.close()
	})
	test('delayed retry', async (t) => {
		let attempts = 0
		const times: number[] = []
		const queue = new Queue({
			delayedRetry: createProgram({ id: 'delayedRetry' }, async () => {
				await step.run({
					name: 'yo',
					retry: {
						attempts: 3,
						delay: 10,
					}
				}, () => {
					times.push(Date.now())
					attempts++
					if (attempts < 3) {
						throw new Error('delayedRetry userland error')
					}
					return 'passed yo step ok'
				})
			})
		})
		await queue.registry.delayedRetry.invoke()
		const deltas = times.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - times[i - 1]!)
			return acc
		}, [])
		assert.strictEqual(attempts, 3, 'Retry should have been attempted 3 times')
		assert.strictEqual(deltas.length, 2, 'There are 2 intervals between 3 retries')
		assert(deltas.every(delta => delta >= 10), 'All retries should have taken at least 10ms')
		t.diagnostic(`deltas: ${deltas.join('ms, ')}ms (3 attempts, delay 10ms)`)
		await queue.close()
	})
})

test.describe('parallel', () => {
	test('with retries', async (t) => {
		let attemptsA = 0
		let attemptsB = 0
		const queue = new Queue({
			parallel: createProgram({ id: 'parallel' }, async () => {
				const [a, b] = await Promise.all([
					step.run('a', async () => {
						await new Promise(r => setTimeout(r, 10))
						if (attemptsA++ === 0) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run('b', () => {
						if (attemptsB++ === 0) {
							throw new Error('b failed')
						}
						return 'b'
					}),
				])
				return { a, b }
			})
		})
		const result = await queue.registry.parallel.invoke()
		assert.strictEqual(result.a, 'a')
		assert.strictEqual(result.b, 'b')
		await queue.close()
	})
	test('with delayed retries', async (t) => {
		let attemptsA = 0
		let attemptsB = 0
		const atimes: number[] = []
		const btimes: number[] = []
		const queue = new Queue({
			parallel: createProgram({ id: 'parallel' }, async () => {
				const [a, b] = await Promise.all([
					step.run({ name: 'a', retry: { attempts: 4, delay: 10 } }, async () => {
						atimes.push(Date.now())
						await new Promise(r => setTimeout(r))
						if (attemptsA++ < 3) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run({ name: 'b', retry: { attempts: 3, delay: 20 } }, () => {
						btimes.push(Date.now())
						if (attemptsB++ < 2) {
							throw new Error('b failed')
						}
						return 'b'
					}),
				])
				return { a, b }
			})
		})
		const result = await queue.registry.parallel.invoke()
		assert.strictEqual(result.a, 'a')
		assert.strictEqual(result.b, 'b')
		const adeltas = atimes.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - atimes[i - 1]!)
			return acc
		}, [])
		const bdeltas = btimes.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - btimes[i - 1]!)
			return acc
		}, [])
		t.diagnostic(`a deltas: ${adeltas.join('ms, ')}ms (4 attempts, delay 10ms)`)
		t.diagnostic(`b deltas: ${bdeltas.join('ms, ')}ms (3 attempts, delay 20ms)`)
		assert.strictEqual(adeltas.length, 3)
		assert.strictEqual(bdeltas.length, 2)
		assert(adeltas.every(delta => delta >= 10), 'All a retries should have taken at least 10ms')
		assert(bdeltas.every(delta => delta >= 20), 'All b retries should have taken at least 20ms')
		assert(adeltas.every(delta => delta < 20), 'All a retries should have taken less than 20ms')
		assert(bdeltas.every(delta => delta < 30), 'All b retries should have taken less than 30ms')
		await queue.close()
	})
	test('with synchronous delayed retries', async (t) => {
		let attemptsA = 0
		let attemptsB = 0
		const atimes: number[] = []
		const btimes: number[] = []
		const queue = new Queue({
			parallel: createProgram({ id: 'parallel' }, async () => {
				const [a, b] = await Promise.all([
					step.run({ name: 'a', retry: { attempts: 3, delay: 20 } }, () => {
						atimes.push(Date.now())
						if (attemptsA++ < 2) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run({ name: 'b', retry: { attempts: 4, delay: 10 } }, () => {
						btimes.push(Date.now())
						if (attemptsB++ < 3) {
							throw new Error('b failed')
						}
						return 'b'
					}),
				])
				return { a, b }
			})
		})
		const result = await queue.registry.parallel.invoke()
		assert.strictEqual(result.a, 'a')
		assert.strictEqual(result.b, 'b')
		const adeltas = atimes.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - atimes[i - 1]!)
			return acc
		}, [])
		const bdeltas = btimes.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - btimes[i - 1]!)
			return acc
		}, [])
		t.diagnostic(`a deltas: ${adeltas.join('ms, ')}ms (3 attempts, delay 20ms)`)
		t.diagnostic(`b deltas: ${bdeltas.join('ms, ')}ms (4 attempts, delay 10ms)`)
		assert.strictEqual(adeltas.length, 2)
		assert.strictEqual(bdeltas.length, 3)
		assert(adeltas.every(delta => delta >= 20), 'All a retries should have taken at least 20ms')
		assert(bdeltas.every(delta => delta >= 10), 'All b retries should have taken at least 10ms')
		assert(adeltas.every(delta => delta < 30), 'All a retries should have taken less than 30ms')
		assert(bdeltas.every(delta => delta < 20), 'All b retries should have taken less than 20ms')
		await queue.close()
	})
})

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

// TODO: test memoization
// TODO: test queue can be killed and recreated from DB
// TODO: fix Test "pokemon" at src/v2/foo.test.ts:1:603 generated asynchronous activity after the test ended. This activity created the error "TypeError: The database connection is not open" and would have caused the test to fail, but instead triggered an unhandledRejection event.


function exhaustQueue(queue: Queue<any>) {
	return new Promise<void>((resolve) => {
		let timeoutId = setTimeout(resolve, 20)
		listenAll(queue, () => {
			clearTimeout(timeoutId)
			timeoutId = setTimeout(resolve, 20)
		})
	})
}

function listenAll(queue: Queue, cb: () => void) {
	const oldEmit = queue.emitter.emit
	// @ts-ignore
	queue.emitter.emit = function () { cb(); oldEmit.apply(queue.emitter, arguments) }
}