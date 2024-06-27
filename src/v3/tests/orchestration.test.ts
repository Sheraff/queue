import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "assert"
import { invoke } from "./utils"
import Database from "better-sqlite3"
import type { Task } from "../lib/storage"

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

test('debounce', { timeout: 500 }, async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
		debounce: { id: 'global', ms: 50 },
	}, async (input) => {
		return { b: input.a }
	})

	const bbb = new Job({
		id: 'bbb',
		input: z.object({ b: z.number() }),
		output: z.object({ c: z.number() }),
		debounce: { id: 'global', ms: 50 },
	}, async (input) => {
		return { c: input.b }
	})

	const db = new Database()
	db.pragma('journal_mode = WAL')

	const queue = new Queue({
		id: 'shoooo',
		jobs: { aaa, bbb },
		storage: new SQLiteStorage({ db })
	})

	const finished: string[] = []

	function getStatuses() {
		const tasks = db.prepare('SELECT * FROM tasks').all() as Task[]
		return tasks.map(task => task.status)
	}

	invoke(queue.jobs.aaa, { a: 1 }).catch(() => finished.push('cancelled-a1'))
	assert.deepEqual(getStatuses(), ['stalled'])
	invoke(queue.jobs.aaa, { a: 2 }).catch(() => finished.push('cancelled-a2'))
	assert.deepEqual(getStatuses(), ['cancelled', 'stalled'])
	invoke(queue.jobs.bbb, { b: 1 }).catch(() => finished.push('cancelled-b1'))
	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'stalled'])

	await new Promise((resolve) => setTimeout(resolve, 10))

	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'stalled'])
	invoke(queue.jobs.bbb, { b: 2 }).catch(() => finished.push('cancelled-b2'))
	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'cancelled', 'stalled'])
	invoke(queue.jobs.aaa, { a: 3 }).then(() => finished.push('success-a3'))
	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'cancelled', 'cancelled', 'stalled'])

	await new Promise((resolve) => setTimeout(resolve, 100))
	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'cancelled', 'cancelled', 'completed'])

	await invoke(queue.jobs.bbb, { b: 3 }).then(() => finished.push('success-b3'))
	assert.deepEqual(getStatuses(), ['cancelled', 'cancelled', 'cancelled', 'cancelled', 'completed', 'completed'])

	assert.deepEqual(finished, [
		'cancelled-a1',
		'cancelled-a2',
		'cancelled-b1',
		'cancelled-b2',
		'success-a3',
		'success-b3',
	], 'jobs finished in descending priority order')

	await queue.close()
})