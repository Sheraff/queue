import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import { invoke } from "./utils"
import assert from "node:assert"
import Database from "better-sqlite3"
import type { Step } from "../lib/storage"
import { z } from "zod"

test('sleep', async (t) => {

	const aaa = new Job({
		id: 'aaa',
	}, async () => {
		await Job.sleep("100 ms")
	})

	const db = new Database()
	db.pragma('journal_mode = WAL')

	const queue = new Queue({
		id: 'sleep',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	let started = 0
	aaa.emitter.on('start', () => started = Date.now())
	let ended = 0
	aaa.emitter.on('success', () => ended = Date.now())
	let continues = 1
	aaa.emitter.on('run', () => continues++)

	await invoke(queue.jobs.aaa, {})

	t.diagnostic(`Sleep took ${ended - started}ms (requested 100ms)`)
	t.diagnostic(`Runs to complete the job: ${continues}`)

	assert.notEqual(started, 0, 'Start event should have been triggered')
	assert.notEqual(ended, 0, 'Success event should have been triggered')
	assert(ended - started >= 100, `Sleep should take at least 100ms, took ${ended - started}ms`)
	assert.equal(continues, 2, 'Sleeping should only require 1 re-run')

	await queue.close()

	const steps = db.prepare('SELECT * FROM steps').all() as Step[]

	assert.equal(steps.length, 1)
	const sleep = steps[0]!
	assert.equal(sleep.status, 'completed')
	assert.equal(sleep.step, 'system/sleep#0')
	//@ts-expect-error -- not exposed in the type
	assert.notEqual(sleep.created_at, sleep.updated_at)

	db.close()
})


test('wait for job event', async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() })
	}, async (input) => {
		const foo = await Job.run('simple', () => input.in)
		return { foo }
	})

	let bDone = false
	const bbb = new Job({
		id: 'bbb',
		output: z.object({ bar: z.number() }),
	}, async () => {
		const data = await Job.waitFor(aaa, 'success', { filter: { in: 2 } })
		bDone = true
		return { bar: data.result.foo }
	})

	const queue = new Queue({
		id: 'wait-for',
		jobs: { aaa, bbb },
		storage: new SQLiteStorage()
	})

	let runs = 1
	queue.jobs.bbb.emitter.on('run', () => runs++)

	const b = invoke(queue.jobs.bbb, {})

	// Even when giving it some time, the job should not be done yet because it's waiting for an event in aaa
	await new Promise(r => setTimeout(r, 20))
	assert.equal(bDone, false, 'Job bbb should not be done yet')

	// Even when giving it some time, the job should not be done yet because the event from aaa does not match the filter
	await invoke(queue.jobs.aaa, { in: 1 })
	await new Promise(r => setTimeout(r, 20))
	assert.equal(bDone, false, 'Job bbb should not be done yet')

	await invoke(queue.jobs.aaa, { in: 2 })
	const result = await b

	t.diagnostic(`Runs to complete the job: ${runs}`)
	assert.equal(runs, 2, 'Job bbb should have been re-run only once')
	assert.equal(bDone, true, 'Job bbb should be done')

	assert.deepEqual(result, { bar: 2 })

	await queue.close()
})

test('wait for pipe event', async (t) => {
	const pipe = new Pipe({
		id: 'pipe',
		input: z.object({ in: z.number() }),
	})

	let done = false
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.string() }),
		output: z.object({ foo: z.number() })
	}, async (input) => {
		const inner = await Job.run('simple', () => Number(input.in))
		const data = await Job.waitFor(pipe)
		done = true
		return { foo: inner + data.in }
	})

	const queue = new Queue({
		id: 'wait-for-pipe',
		jobs: { aaa },
		pipes: { pipe },
		storage: new SQLiteStorage()
	})

	const a = invoke(queue.jobs.aaa, { in: '1' })
	await new Promise(r => setTimeout(r, 20))
	assert.equal(done, false, 'Job aaa should not be done yet')

	queue.pipes.pipe.dispatch({ in: 2 })

	const result = await a

	assert.equal(done, true, 'Job aaa should be done')
	assert.deepEqual(result, { foo: 3 })

	await queue.close()
})

test('invoke', async (t) => {
	let ranA = false
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() })
	}, async (input) => {
		ranA = true
		return { foo: input.in }
	})

	const bbb = new Job({
		id: 'bbb',
		output: z.object({ bar: z.number() }),
	}, async () => {
		const data = await Job.invoke(aaa, { in: 2 })
		return { bar: data.foo }
	})

	const queue = new Queue({
		id: 'invoke',
		jobs: { aaa, bbb },
		storage: new SQLiteStorage()
	})

	const result = await invoke(queue.jobs.bbb, {})

	assert.deepEqual(result, { bar: 2 })
	assert.equal(ranA, true, 'Job aaa should have ran')
})

test('cancel', async (t) => {
	let done = false
	const aaa = new Job({
		id: 'aaa',
	}, async () => {
		await Job.sleep("100 ms")
		done = true
	})

	const bbb = new Job({
		id: 'bbb',
	}, async () => {
		await Job.cancel(aaa, {}, { type: 'explicit' })
	})

	const queue = new Queue({
		id: 'cancel',
		jobs: { aaa, bbb },
		storage: new SQLiteStorage()
	})

	const promise = invoke(queue.jobs.aaa, {})
	await new Promise(r => setTimeout(r, 10))
	await invoke(queue.jobs.bbb, {})
	await promise

	assert.strictEqual(done, false)

})