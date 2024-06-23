import test from "node:test"
import { Job, Queue, SQLiteStorage } from "../lib"
import { invoke } from "./utils"
import assert from "node:assert"
import Database from "better-sqlite3"
import type { Step } from "../lib/storage"

test('sleep', async (t) => {

	const aaa = new Job({
		id: 'aaa',
	}, async () => {
		await Job.sleep(100)
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
	console.log(steps)
	assert.equal(steps.length, 1)
	const sleep = steps[0]!
	assert.equal(sleep.status, 'completed')
	assert.equal(sleep.step, 'system/sleep#0')
	//@ts-expect-error -- not exposed in the type
	assert.notEqual(sleep.created_at, sleep.updated_at)

	db.close()
})