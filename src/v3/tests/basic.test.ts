import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "assert"
import Database from "better-sqlite3"
import type { Step, Task } from "../lib/storage"
import { invoke } from "./utils"

test('simple sync job', async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
	}, async (input) => {
		let next = input.a
		for (let i = 0; i < 10; i++) {
			next = await Job.run('add-one', () => next + 1)
		}
		return { b: next }
	})

	const db = new Database()
	db.pragma('journal_mode = WAL')

	const queue = new Queue({
		id: 'basic',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	performance.mark('start')
	const result = await invoke(queue.jobs.aaa, { a: 1 })
	performance.mark('end')

	t.diagnostic(`Duration: ${performance.measure('test', 'start', 'end').duration.toFixed(2)}ms`)
	assert.deepEqual(result, { b: 11 })

	await queue.close()

	const tasks = db.prepare('SELECT * FROM tasks').all() as Task[]
	assert.strictEqual(tasks.length, 1)
	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 12)
	assert(steps.every(step => step.status === 'completed'))
	assert.strictEqual(steps.filter(s => s.step === 'system/parse-input#0').length, 1)
	assert.strictEqual(steps.filter(s => s.step === 'system/parse-output#0').length, 1)
	assert.strictEqual(steps.filter(s => s.step.startsWith('user/add-one#')).length, 10)

	db.close()
})

test('simple async job', async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
	}, async (input) => {
		let next = input.a
		for (let i = 0; i < 10; i++) {
			next = await Job.run('add-one', async () => next + 1)
		}
		return { b: next }
	})

	const db = new Database()
	db.pragma('journal_mode = WAL')

	const queue = new Queue({
		id: 'basic',
		jobs: { aaa },
		storage: new SQLiteStorage({ db })
	})

	performance.mark('start')
	const result = await invoke(queue.jobs.aaa, { a: 1 })
	performance.mark('end')

	t.diagnostic(`Duration: ${performance.measure('test', 'start', 'end').duration.toFixed(2)}ms`)
	assert.deepEqual(result, { b: 11 })

	await queue.close()

	const tasks = db.prepare('SELECT * FROM tasks').all() as Task[]
	assert.strictEqual(tasks.length, 1)
	const steps = db.prepare('SELECT * FROM steps').all() as Step[]
	assert.strictEqual(steps.length, 12)
	assert(steps.every(step => step.status === 'completed'))
	assert.strictEqual(steps.filter(s => s.step === 'system/parse-input#0').length, 1)
	assert.strictEqual(steps.filter(s => s.step === 'system/parse-output#0').length, 1)
	assert.strictEqual(steps.filter(s => s.step.startsWith('user/add-one#')).length, 10)

	db.close()
})

test.describe('memo', () => {
	test('steps do not re-execute', async (t) => {
		let count = 0
		const aaa = new Job({
			id: 'aaa',
			input: z.object({ a: z.number() }),
			output: z.object({ b: z.number() }),
		}, async (input) => {
			let next = input.a
			next = await Job.run('add-one', () => {
				count++
				return next + 1
			})
			return { b: next }
		})

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})

		queue.jobs.aaa.dispatch({ a: 1 })
		queue.jobs.aaa.dispatch({ a: 1 })
		queue.jobs.aaa.dispatch({ a: 1 })
		queue.jobs.aaa.dispatch({ a: 2 })
		queue.jobs.aaa.dispatch({ a: 2 })
		await invoke(queue.jobs.aaa, { a: 2 })

		assert.strictEqual(count, 2, 'Step should have been memoized')

		await queue.close()
	})
	test('tasks do not re-execute', async (t) => {
		let count = 0
		const aaa = new Job({
			id: 'aaa',
			output: z.object({ b: z.number() }),
		}, async () => {
			const next = await Job.run('add-one', () => {
				count++
				return count
			})
			return { b: next }
		})

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})

		const a = await invoke(queue.jobs.aaa, { a: 1 })
		const b = await invoke(queue.jobs.aaa, { a: 1 })

		assert.deepEqual(a, b, 'Task should have been memoized')

		await queue.close()
	})
})

test.describe('events', () => {
	test('success events', async (t) => {
		const events: string[] = []
		const callbacks: string[] = []
		const aaa = new Job({
			id: 'aaa',
			input: z.object({ a: z.number() }),
			output: z.object({ b: z.number() }),
			onCancel: () => callbacks.push('cancel'),
			onError: () => callbacks.push('error'),
			onSettled: () => callbacks.push('settled'),
			onStart: () => callbacks.push('start'),
			onSuccess: () => callbacks.push('success'),
			onTrigger: () => callbacks.push('trigger'),
		}, async (input) => {
			const next = await Job.run('add-one', async () => input.a + 1)
			return { b: next }
		})

		aaa.emitter.on('trigger', () => events.push('trigger'))
		aaa.emitter.on('start', () => events.push('start'))
		aaa.emitter.on('success', () => events.push('success'))
		aaa.emitter.on('settled', () => events.push('settled'))
		aaa.emitter.on('cancel', () => events.push('cancel'))
		aaa.emitter.on('error', () => events.push('error'))
		aaa.emitter.on('run', () => events.push('run'))

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})

		await invoke(queue.jobs.aaa, { a: 1 })

		assert.deepEqual(events, ['trigger', 'start', 'run', 'success', 'settled'], 'Events should have been emitted in order')
		assert.deepEqual(callbacks, ['trigger', 'start', 'success', 'settled'], 'Callbacks should have been called in order')

		await queue.close()
	})
})

test('step error', async (t) => {
	class CustomError extends Error {
		override name = 'CustomError'
	}

	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
	}, async () => {
		await Job.run('add-one', async () => {
			throw new CustomError('Step error')
		})
	})

	const queue = new Queue({
		id: 'basic',
		jobs: { aaa },
		storage: new SQLiteStorage()
	})


	let runs = 1
	aaa.emitter.on('run', () => runs++)

	await assert.rejects(invoke(queue.jobs.aaa, { a: 1 }), { message: 'Step error', name: 'CustomError' })
	t.diagnostic(`Runs to complete the job: ${runs}`)
	runs = 1
	// @ts-expect-error -- purposefully testing passing a string
	await assert.rejects(invoke(queue.jobs.aaa, { a: '1' }), { message: 'Input parsing failed', name: 'NonRecoverableError' })
	t.diagnostic(`Runs to complete the job: ${runs}`)

	await queue.close()
})
