import assert from "node:assert"
import { test } from 'node:test'
import { foo } from "./userland/foo"
import { Queue } from "./queue"
import { pokemon } from "./userland/pokemon"
import { exhaustQueue } from "./test.utils"

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