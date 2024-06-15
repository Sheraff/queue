import assert from "node:assert"
import { test } from 'node:test'
import { Queue, createProgram, forwardInterrupt, step } from "./queue"


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
	let continues = 0
	queue.emitter.on('system/continue', () => {
		continues++
	})
	await queue.registry.hey.invoke()
	t.diagnostic(`Sleep took ${ended - started}ms (requested 100ms)`)
	assert.notEqual(started, 0, 'Start event should have been triggered')
	assert.notEqual(ended, 0, 'Success event should have been triggered')
	assert(ended - started >= 100, `Sleep should take at least 100ms, took ${ended - started}ms`)
	t.diagnostic(`Continues: ${continues}`)
	assert.equal(continues, 1, 'Continue event should have been triggered once')
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
		})
		await queue.registry.eventualSuccess.invoke()
		assert.strictEqual(successAttempts, 3, 'Retry should have been attempted 3 times')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 3)
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
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
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 3)
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
		})
		await assert.doesNotReject(() => queue.registry.catchable.invoke())
		assert.strictEqual(afterStep, false, 'Step should not have been executed')
		assert.strictEqual(inCatch, true, 'Error should have been caught in userland')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 2)
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
		})
		await queue.registry.delayedRetry.invoke()
		const deltas = times.reduce<number[]>((acc, time, i) => {
			if (i === 0) return acc
			acc.push(time - times[i - 1]!)
			return acc
		}, [])
		t.diagnostic(`deltas: ${deltas.join('ms, ')}ms (3 attempts, delay 10ms)`)
		assert.strictEqual(attempts, 3, 'Retry should have been attempted 3 times')
		assert.strictEqual(deltas.length, 2, 'There are 2 intervals between 3 retries')
		assert(deltas.every(delta => delta >= 10), 'All retries should have taken at least 10ms')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 2)
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
		})
		const result = await queue.registry.parallel.invoke()
		assert.strictEqual(result.a, 'a')
		assert.strictEqual(result.b, 'b')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 2)
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
					step.run({ name: 'a', retry: { attempts: 4, delay: 130 } }, async () => {
						atimes.push(Date.now())
						await new Promise(r => setTimeout(r))
						if (attemptsA++ < 3) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run({ name: 'b', retry: { attempts: 3, delay: 200 } }, () => {
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
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
		t.diagnostic(`a deltas: ${adeltas.join('ms, ')}ms (4 attempts, delay 130ms)`)
		t.diagnostic(`b deltas: ${bdeltas.join('ms, ')}ms (3 attempts, delay 200ms)`)
		assert.strictEqual(adeltas.length, 3)
		assert.strictEqual(bdeltas.length, 2)
		assert(adeltas.every(delta => delta >= 130), 'All a retries should have taken at least 130ms')
		assert(bdeltas.every(delta => delta >= 200), 'All b retries should have taken at least 200ms')
		assert(adeltas.every(delta => delta < 150), 'All a retries should have taken less than 150ms')
		assert(bdeltas.every(delta => delta < 220), 'All b retries should have taken less than 220ms')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 5)
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
					step.run({ name: 'a', retry: { attempts: 3, delay: 200 } }, () => {
						atimes.push(Date.now())
						if (attemptsA++ < 2) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run({ name: 'b', retry: { attempts: 4, delay: 150 } }, () => {
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
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
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
		t.diagnostic(`a deltas: ${adeltas.join('ms, ')}ms (3 attempts, delay 200ms)`)
		t.diagnostic(`b deltas: ${bdeltas.join('ms, ')}ms (4 attempts, delay 150ms)`)
		assert.strictEqual(adeltas.length, 2)
		assert.strictEqual(bdeltas.length, 3)
		assert(adeltas.every(delta => delta >= 200), 'All a retries should have taken at least 200ms')
		assert(bdeltas.every(delta => delta >= 150), 'All b retries should have taken at least 150ms')
		assert(adeltas.every(delta => delta < 420), 'All a retries should have taken less than 400ms')
		assert(bdeltas.every(delta => delta < 480), 'All b retries should have taken less than 450ms')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 5)
		await queue.close()
	})

	test('will not re-execute successful steps', async (t) => {
		let attemptA = 0
		let attemptB = 0
		const queue = new Queue({
			parallel: createProgram({ id: 'parallel' }, async () => {
				const [a, b] = await Promise.all([
					step.run({ name: 'a', retry: { attempts: 99, delay: 10 } }, () => {
						attemptA++
						if (attemptA < 4) {
							throw new Error('a failed')
						}
						return 'a'
					}),
					step.run('b', async () => {
						attemptB++
						await new Promise(r => setTimeout(r, 100))
						return 'b'
					}),
				])
				return { a, b }
			})
		})
		let continues = 0
		queue.emitter.on('system/continue', () => {
			continues++
		})
		performance.mark('start')
		const result = await queue.registry.parallel.invoke()
		performance.mark('end')
		const duration = performance.measure('parallel', 'start', 'end').duration
		assert.strictEqual(result.a, 'a')
		assert.strictEqual(result.b, 'b')
		assert.strictEqual(attemptA, 4, 'Step a should have been attempted 4 times')
		assert.strictEqual(attemptB, 1, 'Step b should have been attempted once')
		t.diagnostic(`Continues: ${continues}`)
		assert.equal(continues, 3)
		t.diagnostic(`Duration: ${duration.toFixed(2)}ms`)
		// TODO: optimize step retries so they can happen in parallel to other still-running steps
		// assert(duration > 100 && duration < 110, 'Duration should be around 100ms: step a exhausted all its retries during step b execution')
		await queue.close()
	})
})
