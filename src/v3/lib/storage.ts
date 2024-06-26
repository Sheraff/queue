import BetterSqlite3 from "better-sqlite3"

type TaskStatus =
	/** task is ready to be picked up */
	| 'pending'
	/** task is being processed, do not pick up */
	| 'running'
	/** task is waiting for a timer or event (retries, debounce, throttle, ...) */
	| 'stalled'
	/** task finished, data is the successful result */
	| 'completed'
	/** task failed, data is the error */
	| 'failed'
	/** task was cancelled, data is the reason */
	| 'cancelled'

export type Task = {
	id: number
	parent_id: number | null
	queue: string
	job: string
	key: string
	input: string
	priority: number
	status: TaskStatus
	runs: number
	created_at: number
	updated_at: number
	started: boolean
	data: string | null
}

type StepStatus =
	/** step is a promise, currently resolving */
	| 'running'
	/** step ran (at least) once, but needs to re-run */
	| 'pending'
	/** step is blocked by a timer (retry, concurrency, sleep) */
	| 'stalled'
	/* step is waiting for an event (waitFor, invoke) */
	| 'waiting'
	/** step finished, data is the result */
	| 'completed'
	/** step failed, data is the error */
	| 'failed'

export type Step = {
	id: number
	task_id: number
	queue: string
	job: string
	key: string
	/**
	 * `system/sleep#1`
	 * `user/my-id#0`
	 */
	step: string
	status: StepStatus
	next_status?: StepStatus | null
	runs: number
	created_at: number

	/** used on write to set a sleep timer */
	sleep_for?: number | null
	/** actual value stored in storage */
	sleep_until?: number | null
	/** computed on read to know if sleep timer expired */
	sleep_done: boolean | null

	wait_for?: string | null // 'job/aaa/settled' | 'pipe/bbb'
	wait_filter?: string | null // '{"input":{},"error":null}'
	wait_retroactive?: boolean | null

	data: string | null
}

export interface Storage {
	/** Close the database connection if any. This should only close it if the database is not external to the Storage instance */
	close(): void | Promise<void>
	/** Simply return the full Task based on unique index queue+job+key */
	getTask<T>(queue: string, job: string, key: string, cb: (task: Task | undefined) => T): T | Promise<T>
	/** Create a new Task, initial status is 'pending'. Returns `true` if the task was created, and `false` if it already existed. */
	addTask<T>(task: { queue: string, job: string, key: string, input: string, parent_id: number | null, priority: number }, cb?: (inserted: boolean) => T): T | Promise<T>
	/**
	 * Exclusive transaction to:
	 * - retrieve the next task to run immediately, if none, return undefined;
	 * - update that next task's status to 'running' (to avoid another worker picking up the same one) and set `started` to true (to trigger the start event);
	 * - retrieve all steps for that task (if any);
	 * - for every retrieved step that needs to update its status (stalled, waiting), update it to the next_status if available;
	 * - retrieve a boolean indicating whether there is another task to run immediately after this one.
	 */
	startNextTask<T>(queue: string, cb: (task: [task: Task, steps: Step[], hasNext: boolean] | undefined) => T): T | Promise<T>
	/**
	 * How long to wait before there is a task to run
	 * (assuming state of storage doesn't change in the meantime).
	 * 
	 * This query follows the same constraints as `startNextTask`,
	 * but only looks for the next task that is waiting for a timer.
	 * (This can include timers for retries, debounce, throttle, sleep, ...)
	 * 
	 * This query is only called if `startNextTask` returns `undefined`,
	 * so it is safe to only look at *future* tasks, and not check if some
	 * are ready to run immediately.
	 */
	nextFutureTask<T>(queue: string, cb: (result: { seconds: number } | undefined) => T): T | Promise<T>
	/** Final update to a task, sets the status and the corresponding data */
	resolveTask<T>(task: { queue: string, job: string, key: string }, status: 'completed' | 'failed' | 'cancelled', data: string | null, cb?: () => T): T | Promise<T>
	/** Set the task back to 'pending' after the step promises it was waiting for resolved. It can be picked up again. */
	requeueTask<T>(task: Task, cb: () => T): T | Promise<T>
	/** Insert or update a step based on unique index queue+job+key+step */
	recordStep<T>(task: Task, step: Pick<Step, 'step' | 'status' | 'data' | 'wait_for' | 'wait_filter' | 'wait_retroactive' | 'runs'> & { sleep_for?: number | null }, cb: () => T): T | Promise<T>
	/** Append event to table */
	recordEvent<T>(queue: string, key: string, input: string, data: string, cb?: () => T): T | Promise<T>
}

export class SQLiteStorage implements Storage {
	#db: BetterSqlite3.Database
	#externalDb: boolean

	constructor({
		db,
		tables,
	}: {
		/** database file name as string, or database instance */
		db?: BetterSqlite3.Database | string
		/** single table name prefix as string, or record of the names for each table */
		tables?: string | {
			tasks?: string
			steps?: string
			events?: string
		}
	} = {}) {
		if (!db || typeof db === 'string') {
			this.#db = new BetterSqlite3(db)
			this.#db.pragma('journal_mode = WAL')
			this.#externalDb = false
		} else {
			this.#db = db
			this.#externalDb = true
		}

		const tasksTable = typeof tables === 'object' ? tables.tasks : tables ? `${tables}_tasks` : 'tasks'
		const stepsTable = typeof tables === 'object' ? tables.steps : tables ? `${tables}_steps` : 'steps'
		const eventsTable = typeof tables === 'object' ? tables.events : tables ? `${tables}_events` : 'events'

		this.#db.exec(/* sql */ `
			CREATE TABLE IF NOT EXISTS ${tasksTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				parent_id INTEGER, -- TODO
				queue TEXT NOT NULL,
				job TEXT NOT NULL,
				key TEXT NOT NULL,
				input JSON NOT NULL,
				priority INTEGER NOT NULL DEFAULT 0,
				status TEXT NOT NULL,
				started INTEGER NOT NULL DEFAULT FALSE,
				runs INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				data JSON
			);
		
			CREATE UNIQUE INDEX IF NOT EXISTS ${tasksTable}_job_key ON ${tasksTable} (queue, job, key);
		
			CREATE TABLE IF NOT EXISTS ${stepsTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				task_id INTEGER NOT NULL,
				queue TEXT NOT NULL,
				job TEXT NOT NULL,
				key TEXT NOT NULL,
				step TEXT NOT NULL,
				status TEXT NOT NULL,
				next_status TEXT,
				runs INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				sleep_until INTEGER,
				wait_for TEXT,
				wait_filter JSON,
				wait_retroactive BOOLEAN,
				data JSON
			);
		
			CREATE UNIQUE INDEX IF NOT EXISTS ${stepsTable}_job_key_step ON ${stepsTable} (queue, job, key, step);
			CREATE INDEX IF NOT EXISTS ${stepsTable}_task_id ON ${stepsTable} (task_id);
		
			CREATE TABLE IF NOT EXISTS ${eventsTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				queue TEXT NOT NULL,
				key TEXT NOT NULL,
				created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				input JSON,
				data JSON
			);
		
			CREATE INDEX IF NOT EXISTS ${eventsTable}_key ON ${eventsTable} (queue, key);
		`)

		this.#getTaskStmt = this.#db.prepare<{ queue: string, job: string, key: string }, Task | undefined>(/* sql */ `
			SELECT *
			FROM ${tasksTable}
			WHERE queue = @queue AND job = @job AND key = @key
		`)

		const getNextTaskStmt = this.#db.prepare<{ queue: string }, Task>(/* sql */ `
			SELECT *
			FROM ${tasksTable} task
			WHERE
				queue = @queue
				AND status = 'pending'
				AND NOT EXISTS (
					SELECT 1
					FROM ${stepsTable} step
					WHERE
						task_id = task.id
						AND ((
							-- step is stalled and sleep timer is not expired
							status = 'stalled'
							AND sleep_until IS NOT NULL
							AND (sleep_until > unixepoch('subsec'))
						) OR (
							-- step is waiting for an event
							status = 'waiting'
							AND wait_for IS NOT NULL
							AND wait_filter IS NOT NULL
							AND NOT EXISTS (
								WITH filter AS ( -- parse the filter JSON into a table
									SELECT *
									FROM json_tree(wait_filter)
									WHERE type != 'null'
								)
								SELECT 1 FROM ${eventsTable} event
								WHERE event.queue = step.queue
								AND event.key = step.wait_for
								AND (
									CASE step.wait_retroactive
									WHEN TRUE THEN 1
									ELSE event.created_at >= step.created_at
									END
								)
								AND NOT EXISTS ( -- check if all filter conditions are met (reverse checking: if a single mismatch, then it's not a match)
									SELECT 1
									FROM filter
									WHERE (
										filter.type = 'object'
										AND (
											json_extract(event.input, filter.fullKey) IS NULL
											OR json_type(json_extract(event.input, filter.fullKey)) != 'object'
										)
									) OR (
										filter.type = 'array'
										AND (
											json_extract(event.input, filter.fullKey) IS NULL
											OR json_type(json_extract(event.input, filter.fullKey)) != 'array'
										)
									) OR (
										filter.type NOT IN ('object', 'array')
										AND (
											json_extract(event.input, filter.fullKey) IS NULL
											OR json_extract(event.input, filter.fullKey) != filter.value
										)
									)
									LIMIT 1
								)
							)
						))
					LIMIT 1
				)
			ORDER BY
				priority DESC,
				created_at ASC
			LIMIT 2
		`)

		this.#getNextFutureTaskStmt = this.#db.prepare<{ queue: string }, { seconds: number }>(/* sql */ `
			SELECT (steps.sleep_until - unixepoch('subsec')) as seconds
			FROM ${tasksTable} tasks
			LEFT JOIN ${stepsTable} steps ON steps.task_id = tasks.id
			WHERE
				tasks.queue = @queue
				AND tasks.status = 'pending'
				AND steps.status = 'stalled'
				AND steps.sleep_until IS NOT NULL
			ORDER BY seconds ASC
			LIMIT 1
		`)

		const reserveTaskStmt = this.#db.prepare<{ id: number }>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = 'running',
				started = TRUE,
				updated_at = unixepoch('subsec')
			WHERE id = @id
		`)

		const getTaskStepDataStmt = this.#db.prepare<{ id: number }, Step>(/* sql */ `
			SELECT
				*,
				CASE
					WHEN sleep_until IS NULL THEN NULL
					WHEN ((sleep_until) <= unixepoch('subsec')) THEN TRUE
					ELSE FALSE
				END sleep_done
			FROM ${stepsTable}
			WHERE
				task_id = @id
		`)

		this.#getNextTaskTx = this.#db.transaction((queue: string) => {
			const [task, next] = getNextTaskStmt.all({ queue })
			if (!task) return
			reserveTaskStmt.run(task)
			const steps = getTaskStepDataStmt.all(task)
			for (let i = 0; i < steps.length; i++) {
				const step = steps[i]!
				if (step.status === 'waiting') {
					const event = matchStepEventStmt.get({ step_id: step.id })
					if (!event) continue
					// @ts-ignore -- TODO: the types of Step in, Step out are a mess
					steps[i] = this.#recordStepStmt.get({
						...step,
						sleep_for: null,
						status: 'completed',
						data: event.data,
					})
				} else if (step.status === 'stalled' && step.sleep_done) {
					if (!step.next_status) continue
					// @ts-ignore -- TODO: the types of Step in, Step out are a mess
					steps[i] = this.#recordStepStmt.get({
						...step,
						sleep_for: null,
						status: step.next_status,
					})
				}
			}
			return [task, steps, !!next] as [Task, Step[], boolean]
		})

		this.#resolveTaskStmt = this.#db.prepare<{ queue: string, job: string, key: string, status: TaskStatus, data: string | null }>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = @status,
				data = @data,
				updated_at = unixepoch('subsec')
			WHERE queue = @queue AND job = @job AND key = @key
		`)

		this.#loopTaskStmt = this.#db.prepare<{ queue: string, job: string, key: string }>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = 'pending',
				updated_at = unixepoch('subsec')
			WHERE queue = @queue AND job = @job AND key = @key
		`)

		this.#addTaskStmt = this.#db.prepare<Task, undefined | 1>(/* sql */ `
			INSERT OR IGNORE
			INTO ${tasksTable} (queue, job, key, input, parent_id, status, priority)
			VALUES (@queue, @job, @key, @input, @parent_id, 'pending', @priority)
			RETURNING 1
		`)

		// create or update step
		this.#recordStepStmt = this.#db.prepare<{
			queue: string
			job: string
			key: string
			step: string
			runs: number
			task_id: number
			status: StepStatus
			next_status: StepStatus | null
			sleep_for: number | null
			wait_for: string | null
			wait_filter: string | null
			wait_retroactive: number | null
			data: string | null
		}>(/* sql */ `
			INSERT INTO ${stepsTable} (
				queue,
				job,
				key,
				step,
				runs,
				task_id,
				status,
				next_status,
				sleep_until,
				wait_for,
				wait_filter,
				wait_retroactive,
				data
			)
			VALUES (
				@queue,
				@job,
				@key,
				@step,
				@runs,
				@task_id,
				@status,
				@next_status,
				CASE @sleep_for WHEN NULL THEN NULL ELSE (unixepoch('subsec') + @sleep_for) END,
				@wait_for,
				@wait_filter,
				@wait_retroactive,
				@data
			)
			ON CONFLICT (queue, job, key, step)
			DO UPDATE SET
				status = @status,
				next_status = @next_status,
				updated_at = unixepoch('subsec'),
				runs = @runs,
				sleep_until = CASE @sleep_for WHEN NULL THEN NULL ELSE (unixepoch('subsec') + @sleep_for) END,
				data = @data
			RETURNING *
		`)

		this.#recordEventStmt = this.#db.prepare<{ queue: string, key: string, input: string, data: string }>(/* sql */ `
			INSERT INTO ${eventsTable} (queue, key, input, data)
			VALUES (@queue, @key, @input, @data)
		`)

		const matchStepEventStmt = this.#db.prepare<{ step_id: number }, { data: string } | undefined>(/* sql */ `
			WITH step AS (
				SELECT *
				FROM ${stepsTable}
				WHERE id = @step_id
			),
			filter AS ( -- parse the filter JSON into a table
				SELECT *
				FROM json_tree(wait_filter)
				WHERE type != 'null'
			)
			SELECT
				event.data data,
				abs(event.created_at - step.created_at) time_distance
			FROM ${eventsTable} event
			LEFT JOIN step ON event.queue = step.queue AND event.key = step.wait_for
			WHERE (
				CASE step.wait_retroactive
				WHEN TRUE THEN 1
				ELSE event.created_at >= step.created_at
				END
			)
			AND NOT EXISTS ( -- check if all filter conditions are met (reverse checking: if a single mismatch, then it's not a match)
				SELECT 1
				FROM filter
				WHERE (
					filter.type = 'object'
					AND (
						json_extract(event.input, filter.fullKey) IS NULL
						OR json_type(json_extract(event.input, filter.fullKey)) != 'object'
					)
				) OR (
					filter.type = 'array'
					AND (
						json_extract(event.input, filter.fullKey) IS NULL
						OR json_type(json_extract(event.input, filter.fullKey)) != 'array'
					)
				) OR (
					filter.type NOT IN ('object', 'array')
					AND (
						json_extract(event.input, filter.fullKey) IS NULL
						OR json_extract(event.input, filter.fullKey) != filter.value
					)
				)
				LIMIT 1
			)
			ORDER BY time_distance ASC
			LIMIT 1
		`)
	}

	#getTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string }, Task | undefined>
	#getNextTaskTx: BetterSqlite3.Transaction<(queue: string) => [task: Task, steps: Step[], hasNext: boolean] | undefined>
	#getNextFutureTaskStmt: BetterSqlite3.Statement<{ queue: string }, { seconds: number } | undefined>
	#resolveTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string, status: TaskStatus, data: string | null }>
	#loopTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string }>
	#addTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string, input: string, parent_id: number | null, priority: number }, undefined | 1>
	#recordStepStmt: BetterSqlite3.Statement<{
		queue: string
		job: string
		key: string
		task_id: number
		step: string
		runs: number
		status: StepStatus
		next_status: StepStatus | null
		sleep_for: number | null
		wait_for: string | null
		wait_filter: string | null
		wait_retroactive: number | null
		data: string | null
	}>
	#recordEventStmt: BetterSqlite3.Statement<{ queue: string, key: string, input: string, data: string }>

	getTask<T>(queue: string, job: string, key: string, cb: (task: Task | undefined) => T): T {
		const task = this.#getTaskStmt.get({ queue, job, key })
		return cb(task)
	}

	addTask<T>(task: { queue: string, job: string, key: string, input: string, parent_id: number | null, priority: number }, cb?: (inserted: boolean) => T): T {
		const inserted = this.#addTaskStmt.get(task)
		return cb?.(Boolean(inserted)) as T
	}

	startNextTask<T>(queue: string, cb: (result: [task: Task, steps: Step[], hasNext: boolean] | undefined) => T): T {
		const result = this.#getNextTaskTx.exclusive(queue)
		return cb(result)
	}

	nextFutureTask<T>(queue: string, cb: (result: { seconds: number } | undefined) => T): T {
		const result = this.#getNextFutureTaskStmt.get({ queue })
		return cb(result)
	}

	resolveTask<T>(task: { queue: string, job: string, key: string }, status: "completed" | "failed" | "cancelled", data: string | null, cb?: () => T): T {
		this.#resolveTaskStmt.run({ queue: task.queue, job: task.job, key: task.key, status, data })
		return cb?.() as T
	}

	requeueTask<T>(task: Task, cb: () => T): T {
		this.#loopTaskStmt.run({ queue: task.queue, job: task.job, key: task.key })
		return cb() as T
	}

	recordStep<T>(task: Task, step: Pick<Step, 'step' | 'status' | 'next_status' | 'data' | 'wait_for' | 'wait_filter' | 'wait_retroactive' | 'runs'> & { sleep_for?: number }, cb: () => T): T {
		this.#recordStepStmt.run({
			queue: task.queue,
			job: task.job,
			key: task.key,
			runs: step.runs,
			task_id: task.id,
			data: step.data,
			status: step.status,
			next_status: step.next_status ?? null,
			sleep_for: step.sleep_for ?? null,
			wait_for: step.wait_for ?? null,
			wait_filter: step.wait_filter ?? null,
			wait_retroactive: Number(step.wait_retroactive) ?? null,
			step: step.step
		})
		return cb()
	}

	recordEvent<T>(queue: string, key: string, input: string, data: string, cb?: () => T): T {
		this.#recordEventStmt.run({ queue, key, input, data })
		return cb?.() as T
	}

	close() {
		if (!this.#externalDb)
			this.#db.close()
	}
}