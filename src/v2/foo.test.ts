import assert from "node:assert"
import { test } from 'node:test'
import { foo } from "./userland/foo.js"
import { Queue, createProgram, step } from "./queue.js"
import { pokemon } from "./userland/pokemon.js"

test('synchronous passing test', (t) => {
	assert.strictEqual(1, 1)
})

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
	console.log('ended - started', ended - started)
	await queue.close()
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