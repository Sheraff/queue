import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "node:assert"

test('pipe trigger', async (t) => {
	const pipe = new Pipe({
		id: 'pipe',
		input: z.object({ in: z.string() }),
	})

	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() }),
		triggers: [pipe.into((input) => ({ in: Number(input.in) }))]
	}, async (input) => {
		return { foo: input.in }
	})

	const queue = new Queue({
		id: 'wait-for-pipe',
		jobs: { aaa },
		pipes: { pipe },
		storage: new SQLiteStorage()
	})

	const done = new Promise((resolve) => {
		aaa.emitter.once('success', ({ result }) => resolve(result))
	})

	queue.pipes.pipe.dispatch({ in: '2' })

	const result = await done

	assert.deepEqual(result, { foo: 2 })

	await queue.close()
})