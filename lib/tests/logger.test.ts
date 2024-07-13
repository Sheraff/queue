import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../src"
import { z } from "zod"
import assert from "assert"
import { invoke } from "./utils"
import { InMemoryLogger, type Log } from "../src/logger"

test('logger foo', { timeout: 500 }, async (t) => {
	const aaa = new Job({
		id: 'aaa',
		input: z.object({ a: z.number() }),
		output: z.object({ b: z.number() }),
	}, async (input) => {
		let next = input.a
		next = await Job.run('add-one', async ({ logger }) => {
			logger.info('adding one')
			return next + 1
		})
		return { b: next }
	})

	const logger = new InMemoryLogger()

	const queue = new Queue({
		id: 'basic',
		jobs: { aaa },
		storage: new SQLiteStorage(),
		logger: logger
	})

	for (let i = 0; i < 5; i++) {
		const result = await invoke(queue.jobs.aaa, { a: i })
		assert.deepEqual(result, { b: i + 1 })
	}

	{
		const logs: Log[] = []
		await logger.get({ queue: queue.id, job: aaa.id, input: JSON.stringify({ a: 0 }) }, (line) => logs.push(line))

		assert.deepEqual(logs.map(l => `${l.key} - ${l.system ? l.payload.event : 'log'}`), [
			'step/aaa/system/parse-input#0 - run',
			'step/aaa/system/parse-input#0 - success',
			'step/aaa/user/add-one#0 - run',
			'step/aaa/user/add-one#0 - log',
			'step/aaa/user/add-one#0 - success',
			'step/aaa/system/parse-output#0 - run',
			'step/aaa/system/parse-output#0 - success'
		])

		assert.deepEqual(logs.filter(l => !l.system)[0]!.payload, 'adding one')

		assert.deepEqual(logs.at(-1)?.payload, {
			data: {
				b: 1
			},
			event: 'success',
			runs: 1
		})
	}

	{
		const logs: Log[] = []
		await logger.get({ queue: queue.id, job: aaa.id, input: JSON.stringify({ a: 1 }) }, (line) => logs.push(line))
		assert.equal(logs.length, 7)
	}

	{
		const logs: Log[] = []
		await logger.get({ queue: queue.id, job: aaa.id, input: JSON.stringify({ a: 5 }) }, (line) => logs.push(line))
		assert.equal(logs.length, 0)
	}

	await queue.close()

})
