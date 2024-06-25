import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "node:assert"
import { invoke } from "./utils"
import Database from "better-sqlite3"

type Step = { step: string, status: string }

test('cancel during Job.run', async (t) => {
	const db = new Database()
	db.pragma('journal_mode = WAL')

	let done = false
	const aaa = new Job({
		id: 'aaa',
		output: z.object({ hello: z.string() }),
	}, async () => {
		await Job.run('bbb', async () => {
			await new Promise(r => setTimeout(r, 100))
			done = true
		})
		return { hello: 'world' }
	})
	const queue = new Queue({
		id: 'cancel',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	const before = Date.now()
	const promise = invoke(queue.jobs.aaa, {})
	await new Promise(r => setTimeout(r, 10))
	queue.jobs.aaa.cancel({}, { type: 'explicit' })
	const result = await promise
	const after = Date.now()

	assert.strictEqual(done, false, 'Step should not have completed')
	assert.notDeepEqual(result, { hello: 'world' }, 'Job should not have completed')

	t.diagnostic(`Task duration: ${after - before}ms`)
	assert(after - before < 100, 'Task should have been cancelled before it completed')

	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 1)
	assert.strictEqual(steps[0]!.status, 'running', 'Step is still running, we do not abort ongoing user-land code')

	await queue.close()
	assert.strictEqual(done, true)

	const stepsAfterClose = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(stepsAfterClose.length, 1)
	assert.strictEqual(stepsAfterClose[0]!.status, 'completed', 'Closing the queue awaits all ongoing promises to finish')

	db.close()
})

test('cancel before Job.run', async (t) => {
	const db = new Database()
	db.pragma('journal_mode = WAL')

	let done = false
	const aaa = new Job({
		id: 'aaa',
		output: z.object({ hello: z.string() }),
	}, async () => {
		await Job.run('bbb', async () => {
			done = true
			await new Promise(r => setTimeout(r, 100))
		})
		return { hello: 'world' }
	})
	const queue = new Queue({
		id: 'cancel',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	const promise = invoke(queue.jobs.aaa, {})
	queue.jobs.aaa.cancel({}, { type: 'explicit' })
	const result = await promise
	assert.strictEqual(done, false)
	assert.notDeepEqual(result, { hello: 'world' })

	await queue.close()

	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 0, 'No steps should have been created')

	db.close()
})


test('cancel during Job.sleep', async (t) => {
	const db = new Database()
	db.pragma('journal_mode = WAL')

	let done = false
	const aaa = new Job({
		id: 'aaa',
		output: z.object({ hello: z.string() }),
	}, async () => {
		await Job.sleep(100)
		done = true
		return { hello: 'world' }
	})
	const queue = new Queue({
		id: 'cancel',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	const promise = invoke(queue.jobs.aaa, {})
	await new Promise(r => setTimeout(r, 10))
	queue.jobs.aaa.cancel({}, { type: 'explicit' })
	const result = await promise
	assert.strictEqual(done, false)
	assert.notDeepEqual(result, { hello: 'world' })

	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 1, 'Only the sleep step should have been created')
	assert.strictEqual(steps[0]?.status, 'stalled', 'Sleep step is sleeping')

	await queue.close()

	const stepsAfterClose = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(stepsAfterClose[0]?.status, 'stalled', 'Sleep step is still sleeping, no active promise maintained the queue open')

	db.close()
})

test('cancel during Job.waitFor', async (t) => {
	const db = new Database()
	db.pragma('journal_mode = WAL')

	const pipe = new Pipe({
		id: 'pipe',
		input: z.object({ hello: z.string() }),
	})

	let done = false
	const aaa = new Job({
		id: 'aaa',
		output: z.object({ hello: z.string() }),
	}, async () => {
		const result = await Job.waitFor(pipe)
		done = true
		return result
	})

	const queue = new Queue({
		id: 'cancel',
		jobs: { aaa },
		pipes: { pipe },
		storage: new SQLiteStorage({ db })
	})

	const promise = invoke(queue.jobs.aaa, {})
	await new Promise(r => setTimeout(r, 10))
	queue.jobs.aaa.cancel({}, { type: 'explicit' })
	queue.pipes.pipe.dispatch({ hello: 'world' })
	const result = await promise
	assert.strictEqual(done, false)
	assert.notDeepEqual(result, { hello: 'world' })

	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 1, 'Only the waitFor step should have been created')
	assert.strictEqual(steps[0]?.status, 'waiting', 'WaitFor step is waiting')

	await queue.close()

	const stepsAfterClose = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(stepsAfterClose[0]?.status, 'waiting', 'WaitFor step is still waiting, no active promise maintained the queue open')

	db.close()
})