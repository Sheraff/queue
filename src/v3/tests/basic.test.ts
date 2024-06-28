import test, { mock } from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "assert"
import Database from "better-sqlite3"
import type { Step, Task } from "../lib/storage"
import { invoke } from "./utils"

test('simple sync job', { timeout: 500 }, async (t) => {
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
	performance.clearMarks()
})

test('simple async job', { timeout: 500 }, async (t) => {
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
	performance.clearMarks()
})

test.describe('memo', { timeout: 500 }, () => {
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

	test('failed steps do not re-execute', async (t) => {
		let count = 0
		const aaa = new Job({
			id: 'aaa',
			input: z.object({ a: z.number() }),
			output: z.object({ b: z.number() }),
		}, async (input) => {
			let next = input.a
			next = await Job.run({ id: 'add-one', retry: 1 }, async () => {
				count++
				throw new Error('Step error')
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
		await invoke(queue.jobs.aaa, { a: 2 }).catch(() => { })

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

	test('failed tasks do not re-execute', async (t) => {
		let count = 0
		const aaa = new Job({
			id: 'aaa',
			output: z.object({ b: z.number() }),
		}, async () => {
			count++
			const next = await Job.run({ id: 'add-one', retry: 1 }, async () => {
				throw new Error('Step error')
			})
			return { b: next }
		})

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})

		await invoke(queue.jobs.aaa, { a: 1 }).catch(() => { })
		const afterFirst = count
		await invoke(queue.jobs.aaa, { a: 1 }).catch(() => { })
		assert.strictEqual(count, afterFirst, 'Task should have been memoized')

		await queue.close()
	})

	test.todo('cancelled tasks do not re-execute', async (t) => {
		// Or should they re-execute?
	})
})

test.describe('events', { timeout: 500 }, () => {
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

test.describe('errors', async () => {
	test('step error', { timeout: 500 }, async (t) => {
		class CustomError extends Error {
			override name = 'CustomError'
		}

		const asyncJob = new Job({
			id: 'asyncJob',
			input: z.object({ a: z.number() }),
		}, async () => {
			await Job.run({ id: 'add-one', backoff: 20 }, async () => {
				throw new CustomError('Step error')
			})
		})

		const syncJob = new Job({
			id: 'syncJob',
			input: z.object({ a: z.number() }),
		}, async () => {
			await Job.run({ id: 'add-one', backoff: 20 }, () => {
				throw new CustomError('Step error')
			})
		})

		const queue = new Queue({
			id: 'basic',
			jobs: { asyncJob, syncJob },
			storage: new SQLiteStorage()
		})


		let runs = 1
		asyncJob.emitter.on('run', () => runs++)
		syncJob.emitter.on('run', () => runs++)

		performance.mark('before-async')
		await assert.rejects(invoke(queue.jobs.asyncJob, { a: 1 }), { message: 'Step error', name: 'CustomError' })
		performance.mark('after-async')
		t.diagnostic(`Runs to complete the job: ${runs} (async)`)
		const asyncDuration = performance.measure('async', 'before-async', 'after-async').duration
		t.diagnostic(`Duration: ${asyncDuration.toFixed(2)}ms`)
		assert(asyncDuration > 40, 'Min. 40ms total: 3 retry implies 2 intervals of 20ms')
		assert(asyncDuration < 60, 'Max. 60ms total: 3 retry implies 2 intervals of 20ms')
		performance.clearMarks()
		assert.strictEqual(runs, 4, 'Job should have been retried 3 times (+1 because async Job.run needs a loop to resolve)')

		runs = 1
		// @ts-expect-error -- purposefully testing passing a string
		await assert.rejects(invoke(queue.jobs.asyncJob, { a: '1' }), { message: 'Input parsing failed', name: 'NonRecoverableError' })
		t.diagnostic(`Runs to complete the job: ${runs} (async, non recoverable)`)
		assert.strictEqual(runs, 1, 'Job should have been retried 0 times')

		runs = 1
		performance.mark('before-sync')
		await assert.rejects(invoke(queue.jobs.syncJob, { a: 1 }), { message: 'Step error', name: 'CustomError' })
		performance.mark('after-sync')
		t.diagnostic(`Runs to complete the job: ${runs} (sync)`)
		const syncDuration = performance.measure('sync', 'before-sync', 'after-sync').duration
		t.diagnostic(`Duration: ${syncDuration.toFixed(2)}ms`)
		assert(syncDuration > 40, 'Min. 40ms total: 3 retry implies 2 intervals of 20ms')
		assert(syncDuration < 60, 'Max. 60ms total: 3 retry implies 2 intervals of 20ms')
		performance.clearMarks()
		assert.strictEqual(runs, 3, 'Job should have been retried 3 times')

		runs = 1
		// @ts-expect-error -- purposefully testing passing a string
		await assert.rejects(invoke(queue.jobs.syncJob, { a: '1' }), { message: 'Input parsing failed', name: 'NonRecoverableError' })
		t.diagnostic(`Runs to complete the job: ${runs} (sync, non recoverable)`)
		assert.strictEqual(runs, 1, 'Job should have been retried 0 times')

		await queue.close()
	})

	test('step error without backoff delay / with custom `retry` fn', { timeout: 500 }, async (t) => {
		let count = 0
		const aaa = new Job({
			id: 'aaa',
			onStart(params) {
				performance.mark(`start`)
			},
			onError(params) {
				performance.mark(`error`)
			},
		}, async () => Job.run({
			id: 'add-one',
			backoff: 0,
			retry(attempt, error) {
				return attempt < 3
			},
		}, async () => {
			count++
			throw new Error('Step error')
		})
		)

		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})

		await invoke(queue.jobs.aaa, { a: 1 }).catch(() => { })
		t.diagnostic(`Runs to complete the job: ${count}`)

		const steps = db.prepare('SELECT * FROM steps').all() as Step[]
		assert.equal(steps.length, 1)
		assert.equal(steps[0]!.status, 'failed')
		assert.equal(steps[0]!.runs, 3)
		assert.equal(count, 3)

		const duration = performance.measure('test', 'start', 'error').duration
		t.diagnostic(`Duration: ${duration.toFixed(2)}ms (< 2ms)`)
		assert(duration < 2, 'Duration should be less than 2ms')

		await queue.close()
		db.close()
		performance.clearMarks()
	})

	test('parsing error (input / output)', { timeout: 500 }, async (t) => {
		const aaa = new Job({
			id: 'aaa',
			input: z.object({ a: z.union([z.number(), z.string()]) }),
			output: z.object({ b: z.number() }),
		}, async (input) => {
			return { b: input.a }
		})

		const db = new Database()
		db.pragma('journal_mode = WAL')

		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage({ db })
		})

		normal: {
			const fn = mock.fn()
			await invoke(queue.jobs.aaa, { a: 1 }).catch(fn)
			assert.equal(fn.mock.calls.length, 0)

			const [input, output, ...rest] = db.prepare('SELECT * FROM steps').all() as Step[]
			assert.equal(input!.status, 'completed')
			assert.equal(input!.step, 'system/parse-input#0')
			assert.equal(output!.status, 'completed')
			assert.equal(output!.step, 'system/parse-output#0')
			assert.equal(rest.length, 0)
			db.exec('DELETE FROM steps')
		}
		input: {
			const fn = mock.fn()
			// @ts-expect-error -- purposefully testing passing an invalid input
			await invoke(queue.jobs.aaa, { a: true }).catch(fn)
			assert.equal(fn.mock.calls.length, 1)

			const [input, output, ...rest] = db.prepare('SELECT * FROM steps').all() as Step[]
			assert.equal(input!.status, 'failed')
			assert.equal(input!.step, 'system/parse-input#0')
			assert.equal(output, undefined)
			assert.equal(rest.length, 0)
			db.exec('DELETE FROM steps')
		}
		output: {
			const fn = mock.fn()
			await invoke(queue.jobs.aaa, { a: '1' }).catch(fn)
			assert.equal(fn.mock.calls.length, 1)

			const [input, output, ...rest] = db.prepare('SELECT * FROM steps').all() as Step[]
			assert.equal(input!.status, 'completed')
			assert.equal(input!.step, 'system/parse-input#0')
			assert.equal(output!.status, 'failed')
			assert.equal(output!.step, 'system/parse-output#0')
			assert.equal(rest.length, 0)
			db.exec('DELETE FROM steps')
		}

		await queue.close()
		db.close()
	})

	test('forward errors w/o userland errors', { timeout: 500 }, async (t) => {
		const aaa = new Job({
			id: 'aaa',
		}, async () => {
			try {
				await Job.sleep(10)
				const a = Job.run('throws', async () => {
					return 1
				})
				return a
			} catch (error) {
				Job.catch(error)
				return 2
			}
		})
		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})
		let runs = 0
		aaa.emitter.on('run', () => runs++)

		const res = await invoke(queue.jobs.aaa, {})

		assert.strictEqual(runs, 2, 'Job took several loops to complete')
		assert.strictEqual(res, 1, 'Job should have returned 1')
	})

	test('forward errors with userland errors', { timeout: 500 }, async (t) => {
		const aaa = new Job({
			id: 'aaa',
		}, async () => {
			try {
				await Job.sleep(10)
				const a = Job.run({
					id: 'throws',
					retry: 2,
					backoff: 10
				}, async () => {
					await new Promise(r => setTimeout(r, 10))
					throw new Error('Userland error')
				})
				return a
			} catch (error) {
				Job.catch(error)
				return 2
			}
		})
		const queue = new Queue({
			id: 'basic',
			jobs: { aaa },
			storage: new SQLiteStorage()
		})
		let runs = 0
		aaa.emitter.on('run', () => runs++)

		const promise = invoke(queue.jobs.aaa, {})

		await assert.rejects(promise, new Error('Userland error'))
		assert.strictEqual(runs, 3, 'Job took several loops to complete')
	})
})

