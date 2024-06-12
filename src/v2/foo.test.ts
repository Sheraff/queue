import assert from "node:assert"
import { test } from 'node:test'
import { foo } from "./userland/foo.js"
import { Queue } from "./queue.js"
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
	queue.close()
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
	queue.close()
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