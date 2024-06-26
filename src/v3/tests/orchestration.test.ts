import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "assert"
import { invoke } from "./utils"

test('priority', { timeout: 500 }, async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
		priority: (input) => input.a,
	}, async (input) => {
		let next = input.a
		next = await Job.run('add-one', () => next + 1)
		return { b: next }
	})

	const queue = new Queue({
		id: 'test',
		jobs: { aaa },
		storage: new SQLiteStorage()
	})

	const finished: number[] = []

	await Promise.all([
		invoke(queue.jobs.aaa, { a: 2 }).then(() => finished.push(2)),
		invoke(queue.jobs.aaa, { a: 10 }).then(() => finished.push(10)),
		invoke(queue.jobs.aaa, { a: 0 }).then(() => finished.push(0)),
		invoke(queue.jobs.aaa, { a: 20 }).then(() => finished.push(20)),
		invoke(queue.jobs.aaa, { a: -5 }).then(() => finished.push(-5)),
	])

	assert.deepEqual(finished, [20, 10, 2, 0, -5], 'jobs finished in descending priority order')

	await queue.close()
})