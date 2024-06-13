import assert from "node:assert"
import { test } from 'node:test'
import { foo } from "./userland/foo.js"
import { Queue, createProgram, forwardInterrupt, step } from "./queue.js"
import { pokemon } from "./userland/pokemon.js"

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
				await step.run('ya', async () => {
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
					await step.run('yo', async () => {
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
})


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