import test from "node:test"
import { Job, Pipe, Queue, SQLiteStorage } from "../lib"
import { z } from "zod"
import assert from "node:assert"

test('pipe trigger (w/ & w/o `into` transform)', { timeout: 500 }, async (t) => {
	const pipe = new Pipe({
		id: 'pipe',
		input: z.object({ in: z.string() }),
	})

	const other = new Pipe({
		id: 'other',
		input: z.object({ in: z.number() }),
	})

	const aaa = new Job({
		id: 'aaa',
		input: z.object({ in: z.number() }),
		output: z.object({ foo: z.number() }),
		triggers: [
			pipe.into((input) => ({ in: Number(input.in) })),
			other,
		]
	}, async (input) => {
		return { foo: input.in }
	})

	const queue = new Queue({
		id: 'wait-for-pipe',
		jobs: { aaa },
		pipes: { pipe, other },
		storage: new SQLiteStorage()
	})

	into: {
		const done = new Promise((resolve) => {
			aaa.emitter.once('success', ({ result }) => resolve(result))
		})
		queue.pipes.pipe.dispatch({ in: '2' })
		const result = await done

		assert.deepEqual(result, { foo: 2 })
	}

	raw: {
		const done = new Promise((resolve) => {
			aaa.emitter.once('success', ({ result }) => resolve(result))
		})
		queue.pipes.other.dispatch({ in: 3 })
		const result = await done

		assert.deepEqual(result, { foo: 3 })
	}

	await queue.close()
})

// skipped because it's too long (min duration for a cron is 1s)
test.skip('cron triggers', { timeout: 3000 }, async (t) => {
	let executed = 0
	const hello = new Job({
		id: 'hello',
		input: z.object({ date: z.string().datetime(), foo: z.number().optional() }),
		cron: ['*/1 * * * * *', '*/1 * * * * *'],
	}, async () => {
		await Job.run('a', () => executed++)
	})

	let triggers = 0
	hello.emitter.on('trigger', () => triggers++)

	const queue = new Queue({
		id: 'cron',
		jobs: { hello },
		storage: new SQLiteStorage()
	})

	await new Promise(r => setTimeout(r, 2100))

	t.diagnostic(`Triggered ${triggers} times`)
	t.diagnostic(`Executed ${executed} times`)
	assert.strictEqual(executed, 2, 'Step should have been executed once')
	assert.strictEqual(executed, triggers, 'Step triggers should be debounced')

	await queue.close()
})

test('cron triggers fails if input does not accept date', { timeout: 500 }, async (t) => {
	assert.throws(() => {
		new Job({
			id: 'hello',
			input: z.object({ foo: z.number() }),
			// @ts-expect-error -- intentionally invalid
			cron: '*/1 * * * * *',
		}, async () => {
			await Job.run('a', () => 1)
		})
	}, new TypeError("Job hello has a cron trigger but its input validator does not accept {date: '<ISO string>'} as an input."))
})