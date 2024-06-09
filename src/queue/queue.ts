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
	done(condition?: (data: this["data"]) => boolean): void
	sleep(ms: number): void
	registerTask<
		P extends keyof Registry,
		K extends string,
	>(
		program: P,
		initialData: Registry[P]['initial'],
		key: K,
		condition?: (data: this["data"]) => boolean
	): asserts this is {
		data: { [key in K]?: Registry[P]['result'] }
	}
	waitForTask<
		P extends keyof Registry,
		K extends string,
	>(
		program: P,
		key: K,
		condition: [path: string, value: GenericSerializable]
	): asserts this is {
		data: { [key in K]?: Registry[P]['result'] }
	}
}

type Task = {
	/** uuid */
	id: string
	program: keyof Registry
	status: 'pending' | 'running' | 'success' | 'failure' | 'sleeping' | 'waiting'
	/** json */
	data: string
	step: number
	retry: number
	concurrency: number
	delay_between_seconds: number
	/** unix timestamp in seconds (float) */
	created_at: number
	/** unix timestamp in seconds (float) */
	updated_at: number
	/** unix timestamp in seconds (float) */
	wakeup_at: number | null
	/** uuid */
	parent_id: string | null
	parent_key: string | null
	wait_for_program: keyof Registry | null
	wait_for_key: string | null
	wait_for_path: string | null
	wait_for_value: string | null
}

/*************************************************/
/* REGISTER ALL PROGRAMS AT THE START OF THE APP */
/*************************************************/

export type ProgramEntry<
	I extends Data = Record<string, never>,
	R extends Data = Record<string, never>
> = {
	initial: I
	result: I & R
}

declare global {
	interface Registry extends Record<string, ProgramEntry<Data, Data>> {
		/**
		 * this is to be augmented by each program
		 */
	}
}

type ProgramOptions = {
	/** default 3 */
	retry: number
	/** default 5000ms */
	retryDelayMs: number | ((attempt: number) => number)
	/** default Infinity, be mindful that setting a concurrency limit can create deadlocks if a task depends on the execution of other tasks of the same program */
	concurrency: number
	/** default 0, any value above 0 will force `concurrency: 1` */
	delayBetweenMs: number
}

type ProgramDefinition = {
	program: (ctx: Ctx<Data>) => (void | Promise<void>)
	options: ProgramOptions
}

const programs = new Map<keyof Registry, ProgramDefinition>()

export function registerProgram<P extends keyof Registry>({
	name,
	program,
	options = {},
}: {
	name: P,
	program: NoInfer<(ctx: Ctx<Registry[P]['initial']>) => (void | Promise<void>)>,
	options?: Partial<ProgramOptions>
}) {
	programs.set(name, {
		program,
		options: {
			retry: 3,
			retryDelayMs: 5_000,
			delayBetweenMs: 0,
			...options,
			concurrency: options.delayBetweenMs ? 1 : (options.concurrency ?? Infinity),
		},
	})
	// TODO: go over existing tasks of this program, and update their options (in case they have changed since the last registration)
}

/********************************************/
/* REGISTER TASKS DURING THE APP'S LIFETIME */
/********************************************/

const asyncLocalStorage = new AsyncLocalStorage<Task>()

const register = db.prepare<{
	id: string
	program: keyof Registry
	data: string
	parent: string | null
	parent_key: string | null
	concurrency: number
	delay_between_seconds: number
}>(/* sql */`
	INSERT INTO tasks (
		id,
		program,
		data,
		parent_id,
		parent_key,
		concurrency,
		delay_between_seconds,
		created_at,
		updated_at
	)
	VALUES (
		@id,
		@program,
		@data,
		@parent,
		@parent_key,
		@concurrency,
		@delay_between_seconds,
		unixepoch ('subsec'),
		unixepoch ('subsec')
	)
`)
export function registerTask<P extends keyof Registry>(id: string, program: P, initialData: Registry[P]['initial'], parentKey?: string) {
	const p = programs.get(program)
	if (!p) throw new Error(`Unknown program: ${program}. Available programs: ${[...programs.keys()].join(', ')}`)
	const parent = asyncLocalStorage.getStore()
	console.log('register', program, parent?.id, parentKey, id)
	register.run({
		id,
		program,
		data: JSON.stringify(initialData),
		parent: parent?.id ?? null,
		parent_key: parentKey ?? null,
		concurrency: p.options.concurrency,
		delay_between_seconds: p.options.delayBetweenMs / 1000,
	})
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
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
`)
const sleepTask = db.prepare<{
	id: string
	seconds: number
	retry: number
	step: number
}, Task>(/* sql */`
	UPDATE tasks
	SET
		step = @step,
		status = 'sleeping',
		wakeup_at = unixepoch ('subsec') + @seconds,
		retry = @retry,
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
`)
const waitTask = db.prepare<{
	id: string
	program: keyof Registry
	key: string
	path: string
	value: string
}, Task>(/* sql */`
	UPDATE tasks
	SET
		step = step + 1,
		status = 'waiting',
		wait_for_program = @program,
		wait_for_key = @key,
		wait_for_path = @path,
		wait_for_value = @value,
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
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
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
`)
const getTask = db.prepare<{
	id: string
}, Task>(/* sql */`
	SELECT * FROM tasks
	WHERE id = @id
`)
async function handleProgram(task: Task, entry: ProgramDefinition) {
	const data = JSON.parse(task.data) as Data

	let step:
		| ['callback', (data: Data) => Promise<Data | void>]
		| ['done', condition?: (data: Data) => boolean]
		| ['sleep', ms: number]
		| ['register', program: keyof Registry, initial: Data, key: string, condition?: (data: Data) => boolean]
		| ['wait', program: keyof Registry, key: string, path: string, value: GenericSerializable]

	find_step: {
		let index = 0
		const foundToken = Symbol('found')
		try {
			await entry.program({
				data,
				step(cb) {
					if (task.step === index++) {
						step = ['callback', cb as any]
						throw foundToken
					}
				},
				done(condition) {
					if (task.step === index++) {
						step = ['done', condition]
						throw foundToken
					}
				},
				sleep(ms) {
					if (task.step === index++) {
						step = ['sleep', ms]
						throw foundToken
					}
				},
				registerTask(program, initialData, key, condition) {
					if (task.step === index++) {
						step = ['register', program, initialData, key, condition]
						throw foundToken
					}
				},
				waitForTask(program, key, condition) {
					if (task.step === index++) {
						step = ['wait', program, key, condition[0], condition[1]]
						throw foundToken
					}
				},
			})
		} catch (e) {
			if (e !== foundToken) throw e
		}
	}

	// run next step
	if (!step!) step = ['done']
	const next = step

	run: {
		if (next[0] === 'sleep') {
			task = sleepTask.get({ id: task.id, seconds: next[1] / 1000, retry: task.retry, step: task.step + 1 })!
			break run
		}

		if (next[0] === 'wait') {
			const [_, program, key, path, value] = next
			task = waitTask.get({ id: task.id, program, key, path: `$.${path}`, value: JSON.stringify({ value }) })!
			break run
		}

		if (next[0] === 'callback') {
			try {
				const augment = await asyncLocalStorage.run(task, () => next[1](data))
				Object.assign(data, augment)
				task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step + 1, status: 'running' })!
			} catch (e) {
				console.error(e)
				if (entry.options.retry > task.retry) {
					const delayMs = typeof entry.options.retryDelayMs === 'function'
						? entry.options.retryDelayMs(task.retry)
						: entry.options.retryDelayMs
					task = sleepTask.get({ id: task.id, seconds: delayMs / 1000, retry: task.retry + 1, step: task.step })!
				} else {
					task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step, status: 'failure' })!
				}
			} finally {
				break run
			}
		}

		if (next[0] === 'done') {
			const isDone = next[1] ? next[1](data) : true
			if (!isDone) {
				task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step + 1, status: 'running' })!
				break run
			}
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
				return result
			})
			task = tx({ id: task.id, data })!
			break run
		}

		if (next[0] === 'register') {
			const [_, program, initial, key, condition] = next
			if (!condition || condition(data)) {
				asyncLocalStorage.run(task, () => registerTask(crypto.randomUUID(), program, initial, key))
			}
			task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step + 1, status: 'pending' })!
			break run
		}

		throw new Error('Unknown step type')
	}

	if (!task) throw new Error('Task went missing during step execution')

	if (task.status === 'running') {
		await handleProgram(task, entry)
	}
}

const resolveWaitForTask = db.prepare<{
	program: keyof Registry
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

const getFirstTask = db.prepare<[], Task>(/* sql */`
SELECT * FROM tasks
WHERE
	-- not a parent of an unfinished child
	id NOT IN (
		SELECT parent_id FROM tasks
		WHERE parent_key IS NOT NULL
			AND status NOT IN ('success', 'failure')
	)
	-- not of the same program if concurrency is reached for this program
	AND (
		step > 0
		OR concurrency = 1e999
		OR concurrency > (
			SELECT COUNT(*)
			FROM tasks AS sibling
			WHERE sibling.program = tasks.program
			AND sibling.step > 0 -- started
			AND sibling.status NOT IN ('success', 'failure') -- not finished
		)
	)
	-- not before delay_between_ms has passed since the last task of the same program finished
	AND (
		step > 0
		OR delay_between_seconds = 0
		OR 0 = (
			SELECT COUNT(*)
			FROM tasks AS sibling
			WHERE sibling.program = tasks.program
			AND sibling.id != tasks.id
			AND sibling.step > 0 -- started
			LIMIT 1
		)
		OR unixepoch('subsec') > delay_between_seconds + (
			SELECT MAX(updated_at)
			FROM tasks AS sibling
			WHERE sibling.program = tasks.program
			AND sibling.id != tasks.id
			AND sibling.status IN ('success', 'failure') -- finished
		)
	)
	-- either pending, or sleeping and it's time to wake up, or waiting and the condition is met
	AND (
		status = 'pending'
		OR (status = 'sleeping' AND wakeup_at < unixepoch('subsec'))
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
	created_at ASC
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
