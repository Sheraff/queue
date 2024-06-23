import test from "node:test"
import { Job, Queue, SQLiteStorage } from "./lib"
import { z } from "zod"
import assert from "assert"
import Database from "better-sqlite3"
import type { Step, Task } from "./lib/storage"

function invoke<J extends Job>(job: J, input: J["in"]): Promise<J['out']> {
	const done = new Promise<J['out']>(r => {
		job.emitter.on('success', (input, output) => {
			r(output)
		})
	})
	job.dispatch(input)
	return done
}


test('basic', async (t) => {
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