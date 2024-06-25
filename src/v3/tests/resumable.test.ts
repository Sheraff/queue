import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "assert"
import Database from "better-sqlite3"
import type { Step, Task } from "../lib/storage"

test('simple sync job', { timeout: 500 }, async (t) => {
	let done = false
	let total = 0

	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
	}, async (input) => {
		let next = input.a
		for (let i = 0; i < 10; i++) {
			next = await Job.run('add-one', async () => {
				total += 1
				return next + 1
			})
			await Job.sleep(1)
		}
		done = true
		return { b: next }
	})

	const buffer = await (async () => {
		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})

		queue.jobs.aaa.dispatch({ a: 1 })
		await new Promise(resolve => setTimeout(resolve, 10))
		await queue.close()
		t.diagnostic('Closing first queue')

		assert.strictEqual(done, false, 'job should not be done yet')

		const tasks = db.prepare('SELECT * FROM tasks').all() as Task[]
		assert.strictEqual(tasks[0]?.status, 'pending', 'job should be pending')

		const steps = db.prepare('SELECT * FROM steps').all() as Step[]
		t.diagnostic(`Ran ${steps.length} steps before closing`)
		assert(steps.length > 1, 'should have ran multiple steps')
		assert(steps.length < 20, 'should not have ran all steps')

		const backup = await db.serialize()
		db.close()

		return backup
	})()

	//////////////

	{
		const db = new Database(buffer)
		db.pragma('journal_mode = WAL')

		const before = db.prepare('SELECT * FROM steps').all() as Step[]
		t.diagnostic(`Resuming from ${before.length} steps`)

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})
		const promise = new Promise(r => queue.jobs.aaa.emitter.on('settled', ({ result }) => r(result)))
		const result = await promise

		assert.strictEqual(done, true, 'job should be done')
		assert.deepEqual(result, { b: 11 }, 'result should be correct')

		const steps = db.prepare('SELECT * FROM steps').all() as Step[]
		t.diagnostic(`Ran ${steps.length} steps in total, across 2 queues and 2 databases`)

		await queue.close()
		db.close()
	}

	t.diagnostic(`Total steps ran: ${total}`)
	assert.strictEqual(total, 10, 'should have ran 10 steps')
})