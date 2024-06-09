import { AsyncLocalStorage } from "async_hooks"
import { db } from "../db/instance.js"

type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | GenericSerializable[] | { [key: string]: GenericSerializable }

type Data = Record<string, GenericSerializable>

// TODO: there should be a way to inject a logger

// TODO: some logic to "delete old tasks" (those that are done, and we did everything we wanted with the data)

// TODO: would be fun to have some observability (graph of tasks, and some metrics)

export interface Ctx<InitialData extends Data = {}> {
	data: Data & InitialData
	task: Task
	step<T extends Data>(
		cb: (
			data: this["data"],
			task: this["task"]
		) => (Promise<T> | Promise<void> | T | void)
	): Ctx<InitialData & T>
	/**
	 * Warning: using `done` in the middle of a program prevents us from ensuring typesafety for the final data shape.
	 * All keys added after `done` should be made optional (`?:`) in the `result` type of the `ProgramEntry` definition.
	 */
	// TODO: should provide `task` in addition to `data`
	done(condition?: (data: this["data"]) => boolean): Ctx<InitialData>
	sleep(ms: number): Ctx<InitialData>
	registerTask<
		P extends keyof Registry,
		K extends string,
	>(
		program: P,
		initialData: Registry[P]['initial'],
		key: K,
		// TODO: should provide `task` in addition to `data`
		condition?: (data: this["data"]) => boolean
		// TODO: there should be a way to specify whether we want to wait for the task to finish or not
	): Ctx<InitialData & { [key in K]?: Registry[P]['result'] }>
	waitForTask<
		P extends keyof Registry,
		K extends string,
	>(
		program: P,
		/** an SQL path to be used in `JSON_EXTRACT`, for example `c[2].f` (without the `$.` prefix) */
		key: K,
		/** the first term is an SQL path to be used in `JSON_EXTRACT`, for example `c[2].f` (without the `$.` prefix) */
		match: [path: string, value: Scalar]
		// TODO: add `condition` to only wait for the task if a condition is met
	): Ctx<InitialData & { [key in K]?: Registry[P]['result'] }>
}

type Task = {
	/** uuid */
	id: string
	program: keyof Registry
	status: 'pending' | 'running' | 'success' | 'failure' | 'sleeping' | 'waiting' | 'stalled'
	/** json */
	data: string
	step: number
	retry: number
	concurrency: number
	delay_between_seconds: number
	priority: number
	/** unix timestamp in seconds (float) */
	created_at: number
	/** unix timestamp in seconds (float) */
	updated_at: number
	/** unix timestamp in seconds (float) */
	started_at: number | null
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
	initial extends Data = Record<string, never>,
	result extends Data = initial
> = {
	initial: initial
	result: initial & result
}

declare global {
	interface Registry {
		/**
		 * this is to be augmented by each program
		 */
	}
}

type ProgramOptions<Name extends keyof Registry = keyof Registry> = {
	/** default 3 */
	retry: number
	/** default 5000ms */
	retryDelayMs: number | ((attempt: number) => number)
	/** default Infinity, be mindful that setting a concurrency limit can create deadlocks if a task depends on the execution of other tasks of the same program */
	concurrency: number
	/** default 0, any value above 0 will force `concurrency: 1` */
	delayBetweenMs: number
	/** default 0 */
	priority: number | ((data: Registry[Name]['initial']) => number)
	// TODO: onError
	// TODO: onRetry
	// TODO: onDone
	// TODO: some cancel mechanism (programmatic cancel)
	// TODO: some pause / resume mechanism (or is cancel/reschedule enough?)
	// TODO: debounce (incoming matches cause previous tasks to be cancelled, new task is scheduled with a delay)
	// TODO: throttle / rate-limit (incoming matches are ignored until the throttle period is over)
	// TODO: timeout (auto cancel after a certain time)
	// TODO: add some schema validation?? (seems like this could be user-land)
}

type Program<D extends Data> = (ctx: Ctx<D>) => Ctx<D>

type ProgramRegistryItem = {
	program: Program<Data>
	options: ProgramOptions
}

const registry = new Map<keyof Registry, ProgramRegistryItem>()

type ProgramDefinition<P extends keyof Registry> = { [name in P]: { program: Program<Registry[P]['initial']>, options: Partial<ProgramOptions> } }

export function defineProgram<Name extends keyof Registry>(
	name: Name,
	options: NoInfer<Partial<ProgramOptions<Name>>>,
	program: NoInfer<(ctx: Ctx<Registry[Name]['initial']>) => Ctx<Registry[Name]['result']>>
): ProgramDefinition<Name> {
	return { [name]: { program, options } } as unknown as ProgramDefinition<Name>
}

const updateOptions = db.prepare<{
	program: keyof Registry
	concurrency: number
	delay_between_seconds: number
}>(/* sql */`
	UPDATE tasks
	SET
		concurrency = @concurrency,
		delay_between_seconds = @delay_between_seconds
	WHERE program = @program
`)
export function registerPrograms(programs: {
	[P in keyof Registry]: {
		program: Program<Registry[P]['initial']>
		options?: Partial<ProgramOptions>
	}
}) {
	for (const [name, { program, options = {} }] of Object.entries(programs) as any) {
		const opts = {
			retry: 3,
			retryDelayMs: 5_000,
			delayBetweenMs: 0,
			priority: 0,
			...options,
			concurrency: options.delayBetweenMs ? 1 : (options.concurrency ?? Infinity),
		}
		registry.set(name, {
			program,
			options: opts,
		})
		// update existing tasks with new options to avoid possible deadlocks
		updateOptions.run({
			program: name,
			concurrency: opts.concurrency,
			delay_between_seconds: opts.delayBetweenMs / 1000,
			// TODO: for priority, we would have to iterate in JS, not doable in SQL
		})
	}
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
	priority: number
}>(/* sql */`
	INSERT INTO tasks (
		id,
		program,
		data,
		parent_id,
		parent_key,
		concurrency,
		delay_between_seconds,
		priority,
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
		@priority,
		unixepoch ('subsec'),
		unixepoch ('subsec')
	)
`)
export function registerTask<P extends keyof Registry>(id: string, program: P, initialData: Registry[P]['initial'], parentKey?: string) {
	const p = registry.get(program)
	if (!p) throw new Error(`Unknown program: ${program}. Available programs: ${[...registry.keys()].join(', ')}`)
	const parent = asyncLocalStorage.getStore()
	console.log('register', program, parent?.id, parentKey, id)
	register.run({
		id,
		program,
		data: JSON.stringify(initialData),
		parent: parent?.id ?? null,
		parent_key: parentKey ? `$.${parentKey}` : null,
		concurrency: p.options.concurrency,
		delay_between_seconds: p.options.delayBetweenMs / 1000,
		priority: typeof p.options.priority === 'function' ? p.options.priority(initialData) : p.options.priority,
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
		updated_at = unixepoch ('subsec'),
		started_at = IFNULL(started_at, unixepoch ('subsec'))
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
const getTask = db.prepare<{
	id: string
}, Task>(/* sql */`
	SELECT * FROM tasks
	WHERE id = @id
`)
async function handleProgram(task: Task, entry: ProgramRegistryItem) {
	const data = JSON.parse(task.data) as Data

	let step:
		| ['callback', (data: Data, task: Task) => Promise<Data | void>]
		| ['done', condition?: (data: Data) => boolean]
		| ['sleep', ms: number]
		| ['register', program: keyof Registry, initial: Data, key: string, condition?: (data: Data) => boolean]
		| ['wait', program: keyof Registry, key: string, path: string, value: GenericSerializable]

	find_step: {
		let index = 0
		const foundToken = Symbol('found')
		try {
			// TODO: use asyncLocalStorage to forbid calling `ctx` methods from inside a `ctx` method
			const ctx: Ctx<Data> = {
				data,
				task,
				step(cb) {
					if (task.step === index++) {
						step = ['callback', cb as any]
						throw foundToken
					}
					return ctx as any
				},
				done(condition) {
					if (task.step === index++) {
						step = ['done', condition]
						throw foundToken
					}
					return ctx
				},
				sleep(ms) {
					if (task.step === index++) {
						step = ['sleep', ms]
						throw foundToken
					}
					return ctx
				},
				registerTask(program, initialData, key, condition) {
					if (task.step === index++) {
						step = ['register', program, initialData, key, condition]
						throw foundToken
					}
					return ctx as any
				},
				waitForTask(program, key, condition) {
					if (task.step === index++) {
						step = ['wait', program, key, condition[0], condition[1]]
						throw foundToken
					}
					return ctx as any
				},
			}
			entry.program(ctx)
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
			task = waitTask.get({ id: task.id, program, key: `$.${key}`, path: `$.${path}`, value: JSON.stringify({ value }) })!
			break run
		}

		if (next[0] === 'callback') {
			try {
				const augment = await asyncLocalStorage.run(task, () => next[1](data, task))
				Object.assign(data, augment)
				task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step + 1, status: 'running' })!
			} catch (e) {
				console.error(e)
				// TODO: create some `FatalError` class to be thrown by user-land code to indicate a task should not be retried
				// TODO: create some `RetryError` class to be thrown by user-land code to set a custom retry delay (e.g. when a 3rd party rate limit is hit)
				if (entry.options.retry > task.retry) {
					const delayMs = typeof entry.options.retryDelayMs === 'function'
						? entry.options.retryDelayMs(task.retry)
						: entry.options.retryDelayMs
					task = sleepTask.get({ id: task.id, seconds: delayMs / 1000, retry: task.retry + 1, step: task.step })!
				} else {
					const tx = db.transaction((params: { id: string, data: Data }) => {
						const result = markTaskDone.get({ id: params.id, data: JSON.stringify(params.data), status: 'failure' })!
						if (result.parent_id && result.parent_key) {
							updateParentAfterDone.get({ data: result.data, status: 'failure', parent_id: result.parent_id, parent_key: result.parent_key })!
						}
						return result
					})
					task = tx({ id: task.id, data })!

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
				const result = markTaskDone.get({ id: params.id, data: JSON.stringify(data), status: 'success' })!
				if (result.parent_id && result.parent_key) {
					updateParentAfterDone.get({ data: result.data, status: 'success', parent_id: result.parent_id, parent_key: result.parent_key })!
				}
				return result
			})
			task = tx({ id: task.id, data })!
			break run
		}

		if (next[0] === 'register') {
			const [_, program, initial, key, condition] = next
			if (!condition || condition(data)) {
				asyncLocalStorage.run(task, () => registerTask(crypto.randomUUID(), program, initial as any, key))
			}
			task = storeTask.get({ id: task.id, data: JSON.stringify(data), step: task.step + 1, status: 'stalled' })!
			break run
		}

		throw new Error('Unknown step type')
	}

	if (!task) throw new Error('Task went missing during step execution')

	if (task.status === 'running') {
		await handleProgram(task, entry)
	}
}

const markTaskDone = db.prepare<{
	id: string
	data: string
	status: "success" | "failure"
}, Task>(/* sql */`
	UPDATE tasks
	SET
		status = @status,
		data = @data,
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
`)
const updateParentAfterDone = db.prepare<{
	data: string
	status: "success" | "failure"
	parent_id: string
	parent_key: string
}, Task>(/* sql */`
	UPDATE tasks
	SET
		-- TODO: is this correct to cascade the failure status? should it be an option?
		status = CASE WHEN @status = 'success' THEN tasks.status ELSE @status END,
		data = JSON_SET(data, @parent_key, json(@data)),
		updated_at = unixepoch ('subsec')
	WHERE id = @parent_id
	RETURNING *
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
			AND sibling.started_at IS NOT NULL
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
			AND sibling.started_at IS NOT NULL
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
		OR (status = 'stalled' AND 0 = (
			SELECT COUNT(*)
			FROM tasks AS child
			WHERE
				child.parent_id = tasks.id
				AND child.status NOT IN ('success', 'failure')
			LIMIT 1
		))
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
	priority DESC,
	created_at ASC,
	id ASC
LIMIT 1
`)
const getTaskCount = db.prepare<[], { count: number }>(/* sql */`
	SELECT COUNT(*) as count FROM tasks
	WHERE status NOT IN ('success', 'failure')
`)
const markRunning = db.prepare<{
	id: string
}, Task>(/* sql */`
	UPDATE tasks
	SET
		status = 'running',
		wakeup_at = NULL,
		updated_at = unixepoch ('subsec')
	WHERE id = @id
	RETURNING *
`)
const resolveWait = db.prepare<{
	id: string
}, Task>(/* sql */`
	UPDATE
		tasks
	SET
		status = 'running',
		data = (
			CASE
				WHEN tasks.wait_for_key IS NOT NULL
				THEN JSON_SET(tasks.data, tasks.wait_for_key, json(child.data))
				ELSE tasks.data
			END
		),
		wait_for_program = NULL,
		wait_for_key = NULL,
		wait_for_path = NULL,
		wait_for_value = NULL,
		updated_at = unixepoch ('subsec')
	FROM
		(
			-- TODO: do we really need to join here? we should already have everything in 'tasks' from the UPDATE query
			SELECT * FROM tasks as child
			LEFT JOIN tasks as parent 
			ON (
				parent.id = @id
				AND child.program = parent.wait_for_program
				AND child.status = 'success'
				AND JSON_EXTRACT(child.data, parent.wait_for_path) = JSON_EXTRACT(parent.wait_for_value, '$.value')
			)
			LIMIT 1
		) AS child
	WHERE
		tasks.id = @id
	RETURNING *
`)
const getNext = db.transaction(() => {
	const task = getFirstTask.get()
	if (!task) return undefined
	if (!task.wait_for_program) {
		// we'll be running this task, mark it as such
		return markRunning.get({ id: task.id })
	} else {
		// this task was picked up because it was waiting for another task that has now completed
		const toto = resolveWait.get({ id: task.id })
		return toto
	}
})
export async function handleNext() {
	const task = getNext()
	if (task) {
		console.log('handle', task.program, task.step, task.data.slice(0, 30) + (task.data.length > 30 ? '…' : ''))
		handleProgram(task, registry.get(task.program)!)
		return "next"
	}
	const count = getTaskCount.get()
	if (count?.count) {
		// console.log('wait', count.count)
		// TODO: we should be able to know "how long before the next task is ready to run"
		// (as long as no new tasks are added in the meantime)
		// This would greatly reduce the need for polling, and the actual polling would be made much more frequent
		// (basically just an iteration with microtask breaks for the event loop)
		return "wait"
	}
	return "done"
}
