import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import { invoke } from "./utils"
import assert from "node:assert"
import Database from "better-sqlite3"
import type { Step } from "../lib/storage"
import { z } from "zod"
import { TimeoutError } from "../lib/utils"

test('sleep', { timeout: 500 }, async (t) => {

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


test('wait for job event', { timeout: 500 }, async (t) => {
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

test('wait for pipe event', { timeout: 500 }, async (t) => {
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
		const data = await Job.waitFor(pipe, { filter: { in: 2 } })
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

	queue.pipes.pipe.dispatch({ in: 1 })
	await new Promise(r => setTimeout(r, 20))
	assert.equal(done, false, 'Job aaa should not be done yet')

	queue.pipes.pipe.dispatch({ in: 2 })
	const result = await a

	assert.equal(done, true, 'Job aaa should be done')
	assert.deepEqual(result, { foo: 3 })

	await queue.close()
})

test('invoke', { timeout: 500 }, async (t) => {
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

test('cancel', { timeout: 500 }, async (t) => {
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

	const promise = invoke(queue.jobs.aaa, {}).catch(err => err)
	await new Promise(r => setTimeout(r, 10))
	await invoke(queue.jobs.bbb, {})
	await promise

	assert.strictEqual(done, false)
})

test('dispatch', async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() })
	}, async (input) => {
		const foo = await Job.run('simple', () => input.in)
		return { foo }
	})

	const bbb = new Job({
		id: 'bbb',
	}, async () => {
		await Job.dispatch(aaa, { in: 1 })
	})

	const queue = new Queue({
		id: 'dispatch',
		jobs: { aaa, bbb },
		storage: new SQLiteStorage()
	})

	const promise = new Promise(r => {
		queue.jobs.aaa.emitter.once('success', ({ result }) => r(result))
	})

	await invoke(queue.jobs.bbb, {})

	const result = await promise
	assert.deepEqual(result, { foo: 1 })

	await queue.close()
})

test.describe('timeout in job step', { timeout: 500 }, () => {
	test('waitFor timeout', async (t) => {
		const pipe = new Pipe({
			id: 'foo',
			input: z.object({ in: z.number() }),
		})

		const aaa = new Job({
			id: 'aaa',
		}, async () => {
			await Job.waitFor(pipe, { timeout: '5ms' })
		})

		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'timeout',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})

		const result = invoke(queue.jobs.aaa, {})

		await assert.rejects(result, new TimeoutError('Step timed out'))
		await queue.close()
	})

	test('invoke timeout', async (t) => {
		const bbb = new Job({
			id: 'bbb',
		}, async () => {
			await Job.run('long', async () => {
				await new Promise(resolve => setTimeout(resolve, 100))
			})
		})

		const aaa = new Job({
			id: 'aaa',
		}, async () => {
			await Job.invoke(bbb, {}, { timeout: '5ms' })
		})

		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'timeout',
			jobs: { aaa, bbb },
			storage: new SQLiteStorage({ db })
		})

		const result = invoke(queue.jobs.aaa, {})

		await assert.rejects(result, new TimeoutError('Step timed out'))
		await queue.close()
	})

	test('run timeout', async (t) => {
		let aborted = false
		const aaa = new Job({
			id: 'aaa',
			onStart() { performance.mark('start') },
			onSettled() { performance.mark('end') }
		}, async () => {
			await Job.run({
				id: 'foo',
				timeout: '5ms',
				retry: 2,
				backoff: 0
			}, async ({ signal }) => {
				if (signal) signal.onabort = () => aborted = true
				await new Promise(resolve => setTimeout(resolve, 100))
			})
		})

		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'timeout',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})

		const result = invoke(queue.jobs.aaa, {})

		await assert.rejects(result, new TimeoutError('Step timed out'))
		assert(aborted, 'Step should have been aborted')

		const duration = performance.measure('run', 'start', 'end').duration
		performance.clearMarks()
		t.diagnostic(`Run took ${duration.toFixed(2)}ms (requested 5ms)`)
		assert(duration < 50, 'Run should have been aborted and not waited for the full 100ms')

		await queue.close()
	})
})

test('thread', async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() })
	}, async (input) => {
		const foo = await Job.thread({
			id: 'foo',
			retry: 0,
		}, async ({ a, b }, { signal }) => {
			await new Promise(resolve => setTimeout(resolve, 200))
			if (signal.aborted) throw new Error('Aborted')
			return a + b
		}, { a: input.in, b: 2 })
		return { foo }
	})

	const queue = new Queue({
		id: 'thread',
		jobs: { aaa },
		storage: new SQLiteStorage()
	})

	const a = invoke(queue.jobs.aaa, { in: 1 })
	const b = invoke(queue.jobs.aaa, { in: 2 })

	await new Promise(r => setTimeout(r, 10))
	queue.jobs.aaa.cancel({ in: 2 }, { type: 'explicit' })

	await assert.rejects(b, { type: 'explicit' })
	assert.deepEqual(await a, { foo: 3 })

})