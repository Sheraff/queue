import { AsyncLocalStorage } from "async_hooks"
import { db } from "../db/instance.js"

type GenericSerializable = string | number | boolean | null | GenericSerializable[] | { [key: string]: GenericSerializable }

export type Data = { [key: string]: GenericSerializable }

export interface Ctx<InitialData extends Data = {}> {
	data: Data & InitialData
	step<T extends Partial<this["data"]>>(
		cb: (
			data: this["data"],
		) => (Promise<T> | Promise<void> | T | void)
	): asserts this is { data: T }
	sleep(seconds: number): void
	registerTask<
		P extends keyof Program,
		K extends string,
	>(
		program: P,
		initialData: Program[P]['initial'],
		key: K,
		condition?: (data: this["data"]) => boolean
	): asserts this is {
		data: { [key in K]?: Program[P]['result'] }
	}
	waitForTask<
		P extends keyof Program,
		K extends string,
	>(
		program: P,
		key: K,
		condition: [path: string, value: GenericSerializable]
	): asserts this is {
		data: { [key in K]?: Program[P]['result'] }
	}
}

type Task = {
	/** uuid */
	id: string
	program: keyof Program
	status: 'pending' | 'running' | 'success' | 'failure' | 'sleeping' | 'waiting'
	/** json */
	data: string
	step: number
	/** datetime */
	created_at: string
	/** datetime */
	updated_at: string
	/** datetime */
	wakeup_at: string | null
	/** uuid */
	parent_id: string | null
	parent_key: string | null
	wait_for_program: keyof Program | null
	wait_for_key: string | null
	wait_for_path: string | null
	wait_for_value: string | null
}

/*************************************************/
/* REGISTER ALL PROGRAMS AT THE START OF THE APP */
/*************************************************/

type BaseProgram = Record<string, {
	initial: Data
	result: Data
	children?: { [key: string]: Data }
}>

declare global {
	interface Program extends BaseProgram {
		/**
		 * this is to be augmented by each program
		 */
	}
}

const programs = new Map<keyof Program, (ctx: Ctx<any>) => (void | Promise<void>)>()

// TODO: add options param with concurrency settings
// TODO: add options param with retry settings
export function registerProgram<P extends keyof Program>(name: P, program: (ctx: Ctx<Program[P]['initial']>) => (void | Promise<void>)) {
	programs.set(name, program)
}

/********************************************/
/* REGISTER TASKS DURING THE APP'S LIFETIME */
/********************************************/

const asyncLocalStorage = new AsyncLocalStorage<Task>()

const register = db.prepare<{
	id: string
	program: keyof Program
	data: string
	parent: string | null
	parent_key: string | null
}>(/* sql */`
	INSERT INTO tasks (id, program, data, parent_id, parent_key)
	VALUES (@id, @program, @data, @parent, @parent_key)
`)
export function registerTask<P extends keyof Program>(id: string, program: P, initialData: Program[P]['initial'], parentKey?: string) {
	if (!programs.has(program)) throw new Error(`Unknown program: ${program}. Available programs: ${[...programs.keys()].join(', ')}`)
	const parent = asyncLocalStorage.getStore()
	console.log('register', program, parent?.id, parentKey, id)
	register.run({ id, program, data: JSON.stringify(initialData), parent: parent?.id ?? null, parent_key: parentKey ?? null })
}

/***************************************/
/* DEPILE TASKS WITH A REOCCURRING JOB */
/***************************************/

const storeTask = db.prepare<{
	id: string
	data: string
	step: number
	status: Task['status']
}, Task>(/* sql */`
	UPDATE tasks
	SET
		data = @data,
		step = @step,
		status = @status,
		updated_at = CURRENT_TIMESTAMP
	WHERE id = @id
	RETURNING *
`)
const sleepTask = db.prepare<{
	id: string
	seconds: number
}>(/* sql */`
	UPDATE tasks
	SET
		step = step + 1,
		status = 'sleeping',
		wakeup_at = unixepoch(CURRENT_TIMESTAMP) + @seconds,
		updated_at = CURRENT_TIMESTAMP
	WHERE id = @id
`)
const waitTask = db.prepare<{
	id: string
	program: keyof Program
	key: string
	path: string
	value: string
}>(/* sql */`
	UPDATE tasks
	SET
		step = step + 1,
		status = 'waiting',
		wait_for_program = @program,
		wait_for_key = @key,
		wait_for_path = @path,
		wait_for_value = @value,
		updated_at = CURRENT_TIMESTAMP
	WHERE id = @id
`)
const resolveWaitTask = db.prepare<{
	id: string
	data: string
}, Task>(/* sql */`
	UPDATE tasks
	SET
		status = 'running',
		data = @data,
		wait_for_program = NULL,
		wait_for_key = NULL,
		wait_for_path = NULL,
		wait_for_value = NULL,
		updated_at = CURRENT_TIMESTAMP
	WHERE id = @id
	RETURNING *
`)
const getTask = db.prepare<{
	id: string
}, Task>(/* sql */`
	SELECT * FROM tasks
	WHERE id = @id
`)
async function handleProgram(task: Task, program: (ctx: Ctx) => (void | Promise<void>)) {
	const data = JSON.parse(task.data) as Data

	const steps: Array<
		| ['callback', (data: Data) => Promise<Data | void>]
		| ['sleep', seconds: number]
		| ['register', program: keyof Program, initial: Data, key: string, condition?: (data: Data) => boolean]
		| ['wait', program: keyof Program, key: string, path: string, value: GenericSerializable]
	> = []

	const ctx: Ctx = {
		data,
		step(cb) {
			steps.push(['callback', cb as any])
		},
		sleep(seconds) {
			steps.push(['sleep', seconds])
		},
		registerTask(program, initialData, key, condition) {
			steps.push(['register', program, initialData, key, condition])
		},
		waitForTask(program, key, condition) {
			steps.push(['wait', program, key, condition[0], condition[1]])
		},
	}

	await program(ctx)

	// run next step
	const next = steps[task.step]
	if (!next) {
		throw new Error('No next step found')
	}

	if (next[0] === 'sleep') {
		sleepTask.run({ id: task.id, seconds: next[1] })
		return
	}

	if (next[0] === 'wait') {
		const [_, program, key, path, value] = next
		waitTask.run({ id: task.id, program, key, path: `$.${path}`, value: JSON.stringify({ value }) })
		return
	}

	if (next[0] === 'callback') {
		try {
			const augment = await asyncLocalStorage.run(task, () => next[1](data))
			Object.assign(data, augment)
		} catch (e) {
			console.error(e)
			storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step, status: 'failure' })
			return
		}
	} else if (next[0] === 'register') {
		const [_, program, initial, key, condition] = next
		if (!condition || condition(data)) {
			asyncLocalStorage.run(task, () => registerTask(crypto.randomUUID(), program, initial, key))
		}
	} else {
		throw new Error('Unknown step type')
	}
	task.step++

	// delete task from DB
	if (task.step === steps.length) {
		console.log('task done', task.id, data)
		const tx = db.transaction((params: { id: string, data: Data }) => {
			const result = storeTask.get({ id: params.id, data: JSON.stringify(params.data), step: task.step, status: 'success' })
			if (result?.parent_id && result?.parent_key) {
				const parent = getTask.get({ id: result.parent_id })
				if (!parent) throw new Error('Parent not found')
				const parentData = JSON.parse(parent.data) as Data
				parentData[result.parent_key] = params.data
				storeTask.run({ id: parent.id, data: JSON.stringify(parentData), step: parent.step, status: parent.status })
			}
		})
		tx({ id: task.id, data })
	} else {
		storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step, status: 'pending' })
	}
}

const resolveWaitForTask = db.prepare<{
	program: keyof Program
	path: string
	value: string
}, Task>(/* sql */`
	SELECT * FROM tasks
	WHERE
		program = @program
		AND status = 'success'
		AND JSON_EXTRACT(data, @path) = JSON_EXTRACT(@value, '$.value')
	LIMIT 1
`)

/**
 * select any task
 * - that is not a parent of an unfinished child (referenced by any other unfinished task under the `parent_id` column)
 * - that is not 'sleeping' ; or its `wakeup_at` is in the past
 * - that is not 'waiting' ; or its `wait_for_program` has a task in `'success'` whose `data[wait_for_path] = wait_for_value`
 */
const getFirstTask = db.prepare<[], Task>(/* sql */`
SELECT * FROM tasks
WHERE
	id NOT IN (
		SELECT parent_id FROM tasks
		WHERE parent_id IS NOT NULL
			AND status NOT IN ('success', 'failure')
	)
	AND (
		status = 'pending'
		OR (status = 'sleeping' AND wakeup_at < unixepoch(CURRENT_TIMESTAMP))
		OR (status = 'waiting' AND 0 < (
			SELECT COUNT(*)
			FROM tasks AS child
			WHERE
				child.program = tasks.wait_for_program
				AND status = 'success'
				AND JSON_EXTRACT(child.data, tasks.wait_for_path) = JSON_EXTRACT(tasks.wait_for_value, '$.value')
			LIMIT 1
		))
	)
ORDER BY
	id ASC,
	created_at ASC,
	updated_at DESC
LIMIT 1
`)
const getTaskCount = db.prepare<[], { count: number }>(/* sql */`
	SELECT COUNT(*) as count FROM tasks
	WHERE status NOT IN ('success', 'failure')
`)
const getNext = db.transaction(() => {
	const task = getFirstTask.get()
	if (!task) return undefined
	// we'll be running this task, mark it as such
	storeTask.run({ id: task.id, data: task.data, step: task.step, status: 'running' })
	if (!task.wait_for_program) return task
	// this task was picked up because it was waiting for another task that has now completed
	const child = resolveWaitForTask.get({ program: task.wait_for_program!, path: task.wait_for_path!, value: task.wait_for_value! })
	if (!child) throw new Error('No child task found')
	// resolve the wait, inject the data
	const data = JSON.parse(task.data) as Data
	data[task.wait_for_key!] = JSON.parse(child.data)
	const resolved = resolveWaitTask.get({ id: task.id, data: JSON.stringify(data) })
	if (!resolved) throw new Error('No task found')
	return resolved
})
export async function handleNext() {
	const task = getNext()
	if (task) {
		console.log('handle', task.id, task.program, task.step)
		handleProgram(task, programs.get(task.program)!)
		return "next"
	}
	const count = getTaskCount.get()
	if (count?.count) {
		return "wait"
	}
	return "done"
}
