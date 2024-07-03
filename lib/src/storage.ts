import BetterSqlite3 from "better-sqlite3"

export type TaskStatus =
	/** task is ready to be picked up */
	| 'pending'
	/** task is being processed, do not pick up */
	| 'running'
	/** task is waiting for a timer (retries, debounce, throttle, ...) */
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
	timeout_at: number | null
	timed_out?: boolean | null
	status: TaskStatus
	loop: number
	created_at: number
	updated_at: number
	started_at: number | null
	data: string | null
}

export type StepStatus =
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
	discovered_on: number

	/** used on write to set a sleep timer */
	sleep_for?: number | null
	/** actual value stored in storage */
	sleep_until?: number | null
	/** computed on read to know if sleep timer expired */
	sleep_done: boolean | null

	/** used on write */
	timeout: number | null
	/** actual value stored in storage */
	timeout_at: number | null
	/** computed on read */
	timed_out: boolean | null

	wait_for?: string | null // 'job/aaa/settled' | 'pipe/bbb'
	wait_filter?: string | null // '{"input":{},"error":null}'
	/** used on write (actual stored value is `wait_from` as a timestamp) */
	wait_retroactive?: boolean | null

	data: string | null
}

export type Event = {
	queue: string,
	key: string,
	created_at: number,
	input: string,
	data: string,
}

export interface Storage {
	/** sets up the db, useful for async version of storage as a continuation of the constructor */
	init(): void | Promise<void>
	/** Close the database connection if any. This should only close it if the database is not external to the Storage instance */
	close(): void | Promise<void>
	/** Simply return the full Task based on unique index queue+job+key */
	getTask<T>(queue: string, job: string, key: string, cb: (task: Task | undefined) => T): T | Promise<T>
	/** Create a new Task, initial status is 'pending'. Returns `true` if the task was created, and `false` if it already existed. */
	addTask<T>(task: {
		queue: string,
		job: string,
		key: string,
		input: string,
		parent_id: number | null,
		priority: number,
		/** incoming tasks with the same ID are 'stalled' for s seconds and cancel existing 'stalled' tasks with that ID (ID = debounce_id) */
		debounce: { s: number, id: string } | null,
		/** incoming tasks with the same ID are 'stalled' immediately until existing tasks with that ID are started AND s seconds have passed (ID = throttle_id) */
		throttle: { s: number, id: string } | null,
		/** incoming tasks with the same ID are 'cancelled' immediately if there are existing tasks with that ID are started AND fewer than s seconds have passed (ID = rate_limit_id) */
		rateLimit: { s: number, id: string } | null,
		timeout: number | null
	}, cb?: (
		rateLimit: number | null,
		inserted: boolean,
		cancelled?: Task
	) => T): T | Promise<T>
	/**
	 * Exclusive transaction to:
	 * - resolve all step events that are waiting for a specific event to happen;
	 * - retrieve the next task to run immediately, if none, return undefined;
	 * - update that next task's status to 'running' (to avoid another worker picking up the same one) and set `started_at` to now (to trigger the start event);
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
	nextFutureTask<T>(queue: string, cb: (result: { ms: number | null }) => T): T | Promise<T>
	/** Final update to a task, sets the status and the corresponding data */
	resolveTask<T>(task: { queue: string, job: string, key: string }, status: 'completed' | 'failed' | 'cancelled', data: string | null, cb?: () => T): T | Promise<T>
	/** Set the task back to 'pending' after the step promises it was waiting for resolved. It can be picked up again. */
	requeueTask<T>(task: Task, cb: () => T): T | Promise<T>
	/** Insert or update a step based on unique index queue+job+key+step */
	recordStep<T>(task: Task, step: Pick<Step, 'step' | 'status' | 'data' | 'wait_for' | 'wait_filter' | 'wait_retroactive' | 'runs'> & {
		sleep_for?: number | null
		timeout?: number | null
	}, cb: () => T): T | Promise<T>
	/** Append event to table */
	recordEvent<T>(queue: string, key: string, input: string, data: string, cb?: () => T): T | Promise<T>
}

export class SQLiteStorage implements Storage {
	#db: BetterSqlite3.Database
	#externalDb: boolean
	#initialized = false

	#tasksTable: string
	#stepsTable: string
	#eventsTable: string

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
			this.#externalDb = false
		} else {
			this.#db = db
			this.#externalDb = true
		}

		this.#tasksTable = typeof tables === 'object' ? tables.tasks ?? 'tasks' : tables ? `${tables}_tasks` : 'tasks'
		this.#stepsTable = typeof tables === 'object' ? tables.steps ?? 'steps' : tables ? `${tables}_steps` : 'steps'
		this.#eventsTable = typeof tables === 'object' ? tables.events ?? 'events' : tables ? `${tables}_events` : 'events'
	}

	init(): void {
		if (this.#initialized) return
		this.#initialized = true

		if (!this.#externalDb) {
			this.#db.pragma('journal_mode = WAL')
			this.#db.pragma('busy_timeout = 100')
			this.#db.pragma('synchronous = NORMAL')
			this.#db.pragma('cache_size = 2000')
			this.#db.pragma('temp_store = MEMORY')
		}

		const tasksTable = this.#tasksTable
		const stepsTable = this.#stepsTable
		const eventsTable = this.#eventsTable

		this.#db.exec(/* sql */ `
			CREATE TABLE IF NOT EXISTS ${tasksTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				parent_id INTEGER, -- TODO
				queue TEXT NOT NULL,
				job TEXT NOT NULL,
				key TEXT NOT NULL,
				input JSON NOT NULL,

				priority INTEGER NOT NULL DEFAULT 0,
				timeout_at REAL,

				debounce_id TEXT,
				sleep_until REAL,

				throttle_id TEXT,
				throttle_duration INTEGER,

				rate_limit_id TEXT,
				
				status TEXT NOT NULL,
				started_at REAL,
				loop INTEGER NOT NULL DEFAULT 0,
				created_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
				updated_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
				data JSON
			);

			-- base
			CREATE UNIQUE INDEX IF NOT EXISTS ${tasksTable}_job_key ON ${tasksTable} (queue, job, key);
			CREATE UNIQUE INDEX IF NOT EXISTS ${tasksTable}_sibling ON ${tasksTable} (queue, id);
			CREATE INDEX IF NOT EXISTS ${tasksTable}_sort ON ${tasksTable} (queue, priority DESC, created_at ASC);

			-- future
			CREATE INDEX IF NOT EXISTS ${tasksTable}_future_pending ON ${tasksTable} (queue, status) WHERE status = 'pending';
			CREATE INDEX IF NOT EXISTS ${tasksTable}_future_sleep ON ${tasksTable} (queue, status, sleep_until ASC) WHERE sleep_until IS NOT NULL AND status = 'stalled';
			CREATE INDEX IF NOT EXISTS ${tasksTable}_future_throttled ON ${tasksTable} (queue, throttle_id, status, started_at, throttle_duration) WHERE throttle_id IS NOT NULL AND started_at IS NULL AND status = 'stalled';
			CREATE INDEX IF NOT EXISTS ${tasksTable}_future_timed_out ON ${tasksTable} (queue, timeout_at ASC, status) WHERE timeout_at IS NOT NULL AND status IN ('pending', 'stalled');
			-- missing some index for "sibling" in future>throttled

			-- next
			CREATE INDEX IF NOT EXISTS ${tasksTable}_next_main ON ${tasksTable} (queue, id, status, throttle_id, throttle_duration) WHERE status IN ('pending', 'stalled');
			CREATE INDEX IF NOT EXISTS ${tasksTable}_next_throttled_sibling ON ${tasksTable} (queue, throttle_id, started_at) WHERE throttle_id IS NOT NULL AND started_at IS NOT NULL;

			-- other
			CREATE INDEX IF NOT EXISTS ${tasksTable}_debounce_index ON ${tasksTable} (queue, debounce_id, started_at, status) WHERE status = 'stalled' AND started_at IS NULL AND debounce_id IS NOT NULL;
			CREATE INDEX IF NOT EXISTS ${tasksTable}_rate_limit_index ON ${tasksTable} (queue, rate_limit_id, created_at DESC) WHERE rate_limit_id IS NOT NULL;

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
				created_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
				updated_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
				discovered_on INTEGER NOT NULL,
				sleep_until REAL,
				timeout_at REAL,
				wait_for TEXT,
				wait_filter JSON,
				wait_from REAL,
				data JSON
			);
		
			CREATE UNIQUE INDEX IF NOT EXISTS ${stepsTable}_job_key_step ON ${stepsTable} (queue, job, key, step);
			CREATE INDEX IF NOT EXISTS ${stepsTable}_sleep ON ${stepsTable} (task_id, status, sleep_until ASC) WHERE sleep_until IS NOT NULL AND status = 'stalled'; -- future>pending
			CREATE INDEX IF NOT EXISTS ${stepsTable}_task_id ON ${stepsTable} (task_id, status, timeout_at ASC) WHERE timeout_at IS NOT NULL AND status IN ('stalled', 'waiting'); -- future>step_timed_out
			CREATE INDEX IF NOT EXISTS ${stepsTable}_resolve_step_event_covering on ${stepsTable} (queue, status, wait_for, wait_filter, wait_from, id) WHERE status = 'waiting' AND wait_for IS NOT NULL; -- resolve step events
			CREATE INDEX IF NOT EXISTS ${stepsTable}_blocking_sub_steps ON ${stepsTable} (status, task_id, timeout_at, sleep_until, wait_for, wait_filter) WHERE status IN ('stalled', 'waiting'); -- next>sub_steps

		
			CREATE TABLE IF NOT EXISTS ${eventsTable} (
				queue TEXT NOT NULL,
				key TEXT NOT NULL,
				created_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
				input JSON,
				data JSON
			);
		
			-- TODO: is it overkill to put the whole row in the index?
			-- queue/key/created_at is likely to be unique,
			-- so input and data shouldn't add in complexity when computing the index
			-- and this allows us to ge a covering index on the hot path
			CREATE INDEX IF NOT EXISTS ${eventsTable}_key ON ${eventsTable} (queue, key, created_at ASC, input, data);
		`)

		this.#getTaskStmt = this.#db.prepare<{ queue: string, job: string, key: string }, Task | undefined>(/* sql */ `
			SELECT
				*,
				CASE
					WHEN timeout_at IS NULL THEN NULL
					WHEN (timeout_at <= unixepoch('subsec')) THEN TRUE
					ELSE FALSE
				END timed_out
			FROM ${tasksTable}
			WHERE queue = @queue AND job = @job AND key = @key
		`)

		const getNextTaskStmt = this.#db.prepare<{ queue: string }, Task>(/* sql */ `
			WITH queue_tasks AS (
				SELECT
					*
				FROM ${tasksTable}
				WHERE queue = @queue
					AND status IN ('pending', 'stalled')
			),
			sub_steps AS (
				SELECT
					task.id,
					MAX(
						step.timeout_at IS NOT NULL
						AND step.timeout_at <= unixepoch('subsec')
					) AS step_timed_out,
					MAX(
						step.status = 'stalled'
						AND step.sleep_until IS NOT NULL
						AND (step.sleep_until > unixepoch('subsec'))
					) AS step_sleeping,
					MAX(
						step.status = 'waiting'
						AND step.wait_for IS NOT NULL
						AND step.wait_filter IS NOT NULL
					) AS step_waiting
				FROM queue_tasks task
				LEFT JOIN ${stepsTable} step
					ON step.task_id = task.id
				WHERE step.status IN ('stalled', 'waiting')
				GROUP BY task.id
			),
			throttle_sibling AS (
				SELECT
					task.id,
					COUNT(sibling.id) > 0 AS is_throttled
				FROM queue_tasks task
				LEFT JOIN ${tasksTable} sibling
					ON sibling.queue = task.queue
					AND sibling.throttle_id = task.throttle_id
					AND sibling.id != task.id
				WHERE task.throttle_id IS NOT NULL
					AND task.status = 'stalled'
					AND sibling.started_at IS NOT NULL
					AND sibling.started_at > unixepoch('subsec') - task.throttle_duration
				GROUP BY task.id
			)

			SELECT
				task.*,
				CASE
					WHEN timeout_at IS NULL THEN NULL
					WHEN (timeout_at <= unixepoch('subsec')) THEN TRUE
					ELSE FALSE
				END timed_out
			FROM queue_tasks task
			LEFT JOIN sub_steps ON sub_steps.id = task.id
			LEFT JOIN throttle_sibling ON throttle_sibling.id = task.id
			WHERE (
				-- task timed out, resolve it
				timeout_at IS NOT NULL
				AND timeout_at <= unixepoch('subsec')
			) OR (
				-- step timed out, resolve it
				step_timed_out IS 1
			) OR (
				(
					status = 'pending'
					OR (
						-- task was sleeping, e.g. debounced
						sleep_until IS NOT NULL
						AND sleep_until <= unixepoch('subsec')
					)
					OR (
						-- task was throttled
						throttle_id IS NOT NULL
						AND task.throttle_id IS NOT NULL
						AND task.status = 'stalled'
						AND (is_throttled IS NOT 1)
					)
				) AND (
					-- no steps are blocking (sleeping, waiting)
					step_sleeping IS NOT 1
					AND step_waiting IS NOT 1
				)
			)
			ORDER BY
				priority DESC,
				created_at ASC
			LIMIT 2
		`)

		this.#getNextFutureTaskStmt = this.#db.prepare<{ queue: string }, { ms: number }>(/* sql */ `
			WITH
				pending AS (
					SELECT (step.sleep_until) as timeout
					FROM ${tasksTable} task
					LEFT JOIN ${stepsTable} step
					ON step.task_id = task.id
					WHERE
						task.queue = @queue
						AND task.status = 'pending'
						AND step.status = 'stalled'
						AND step.sleep_until IS NOT NULL
					ORDER BY timeout ASC
					LIMIT 1
				),
				sleeping AS (
					SELECT (task.sleep_until) as timeout
					FROM ${tasksTable} task
					WHERE
						task.queue = @queue
						AND task.status = 'stalled'
						AND task.sleep_until IS NOT NULL
					ORDER BY timeout ASC
					LIMIT 1
				),
				throttled AS (
					SELECT (sibling.started_at + task.throttle_duration) as timeout
					FROM ${tasksTable} task
					LEFT JOIN ${tasksTable} sibling
					ON
						sibling.queue = task.queue
						AND sibling.throttle_id = task.throttle_id
						AND sibling.id != task.id
					WHERE
						task.queue = @queue
						AND task.status = 'stalled'
						AND task.throttle_id IS NOT NULL
						AND task.started_at IS NULL
						AND sibling.started_at IS NOT NULL
					ORDER BY timeout ASC
					LIMIT 1
				),
				timed_out AS (
					SELECT (timeout_at) as timeout
					FROM ${tasksTable}
					WHERE
						queue = @queue
						AND status IN ('pending', 'stalled')
						AND timeout_at IS NOT NULL
					ORDER BY timeout ASC
					LIMIT 1
				),
				step_timed_out AS (
					SELECT (step.timeout_at) as timeout
					FROM ${tasksTable} task
					LEFT JOIN ${stepsTable} step
					ON step.task_id = task.id
					WHERE
						task.queue = @queue
						AND task.status IN ('pending', 'stalled')
						AND step.status IN ('stalled', 'waiting')
						AND step.timeout_at IS NOT NULL
					ORDER BY timeout ASC
					LIMIT 1
				)
			SELECT CEIL((MIN(timeout) - unixepoch('subsec')) * 1000) as ms
			FROM (
				SELECT timeout FROM pending
				UNION ALL
				SELECT timeout FROM sleeping
				UNION ALL
				SELECT timeout FROM throttled
				UNION ALL
				SELECT timeout FROM timed_out
				UNION ALL
				SELECT timeout FROM step_timed_out
			) AS combined
			LIMIT 1
		`)

		const reserveTaskStmt = this.#db.prepare<{ id: number }>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = 'running',
				started_at = CASE WHEN started_at IS NULL THEN unixepoch('subsec') ELSE started_at END,
				updated_at = unixepoch('subsec'),
				loop = loop + 1
			WHERE id = @id
		`)

		const getTaskStepDataStmt = this.#db.prepare<{ id: number }, Step>(/* sql */ `
			SELECT
				*,
				CASE
					WHEN sleep_until IS NULL THEN NULL
					WHEN ((sleep_until) <= unixepoch('subsec')) THEN TRUE
					ELSE FALSE
				END sleep_done,
				CASE
					WHEN timeout_at IS NULL THEN NULL
					WHEN ((timeout_at) <= unixepoch('subsec')) THEN TRUE
					ELSE FALSE
				END timed_out
			FROM ${stepsTable}
			WHERE
				task_id = @id
		`)

		this.#getNextTaskTx = this.#db.transaction((queue: string) => {
			// performance.mark('st-start')
			resolveAllStepEventsStmt.get({ queue })
			// performance.mark('st-events')
			const [task, next] = getNextTaskStmt.all({ queue })
			// performance.mark('st-tasks')
			if (!task) {
				// console.log('no task')
				return
			}
			reserveTaskStmt.run(task)
			// performance.mark('st-reserve')
			const steps = getTaskStepDataStmt.all(task)
			// performance.mark('st-steps')
			for (let i = 0; i < steps.length; i++) {
				const step = steps[i]!
				if (step.status === 'stalled' && step.sleep_done) {
					if (!step.next_status) continue
					// @ts-ignore -- TODO: the types of Step in, Step out are a mess
					steps[i] = this.#recordStepStmt.get({
						...step,
						sleep_for: null,
						timeout: null,
						wait_retroactive: null,
						status: step.next_status,
					})
				}
			}
			// performance.mark('st-update')
			{
				// const events = performance.measure('events', 'st-start', 'st-events').duration
				// const tasks = performance.measure('tasks', 'st-events', 'st-tasks').duration
				// const reserve = performance.measure('reserve', 'st-tasks', 'st-reserve').duration
				// const steps = performance.measure('steps', 'st-reserve', 'st-steps').duration
				// const update = performance.measure('update', 'st-steps', 'st-update').duration
				// console.log(`events: ${events.toFixed(2)}ms, tasks: ${tasks.toFixed(2)}ms, reserve: ${reserve.toFixed(2)}ms, steps: ${steps.toFixed(2)}ms, update: ${update.toFixed(2)}ms`)
				// performance.clearMarks('st-start')
				// performance.clearMarks('st-events')
				// performance.clearMarks('st-tasks')
				// performance.clearMarks('st-reserve')
				// performance.clearMarks('st-steps')
				// performance.clearMarks('st-update')
				// performance.clearMeasures()
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

		const checkLatestRateLimitStmt = this.#db.prepare<{ queue: string, rate_limit_id: string, rate_limit_duration: number }, { ms: number }>(/* sql */ `
			SELECT CEIL((@rate_limit_duration + created_at - unixepoch('subsec')) * 1000) ms
			FROM ${tasksTable}
			WHERE
				queue = @queue
				AND rate_limit_id = @rate_limit_id
				AND created_at > unixepoch('subsec') - @rate_limit_duration
			ORDER BY created_at DESC
			LIMIT 1
		`)

		const addTaskStmt = this.#db.prepare<{
			queue: string,
			job: string,
			key: string,
			input: string,
			parent_id: number | null,
			priority: number,
			timeout: number | null,
			debounce_id: string | null,
			throttle_id: string | null,
			rate_limit_id: string | null,
			throttle_duration: number | null,
			sleep_for: number | null,
			status: TaskStatus
		}, undefined | { id: number }>(/* sql */ `
			INSERT OR IGNORE
			INTO ${tasksTable} (queue, job, key, input, parent_id, status, priority, timeout_at, debounce_id, throttle_id, rate_limit_id, throttle_duration, sleep_until)
			VALUES (
				@queue,
				@job,
				@key,
				@input,
				@parent_id,
				CASE WHEN (@throttle_id IS NOT NULL) THEN 'stalled' ELSE @status END,
				@priority,
				CASE @timeout WHEN NULL THEN NULL ELSE (unixepoch('subsec') + @timeout) END,
				@debounce_id,
				@throttle_id,
				@rate_limit_id,
				@throttle_duration,
				CASE @sleep_for WHEN NULL THEN NULL ELSE (unixepoch('subsec') + @sleep_for) END
			)
			RETURNING id
		`)
		const cancelMatchingDebounceStmt = this.#db.prepare<{ debounce_id: string, queue: string, new_id: number }, Task>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = 'cancelled',
				data = '{"type":"debounce"}',
				updated_at = unixepoch('subsec')
			WHERE
				queue = @queue
				AND debounce_id = @debounce_id
				AND status = 'stalled'
				AND started_at IS NULL
				AND id != @new_id
			RETURNING *
		`)

		this.#addTaskTx = this.#db.transaction((task: {
			queue: string
			job: string
			key: string
			input: string
			parent_id: number | null
			priority: number
			timeout: number | null
			debounce: { s: number; id: string } | null
			throttle: { s: number; id: string } | null
			rateLimit: { s: number; id: string } | null
		}) => {
			if (task.rateLimit) {
				const limit = checkLatestRateLimitStmt.get({
					queue: task.queue,
					rate_limit_id: task.rateLimit.id,
					rate_limit_duration: task.rateLimit.s
				})
				if (limit) return [limit.ms, false, undefined]
			}
			const inserted = addTaskStmt.get({
				...task,
				throttle_id: task.throttle?.id ?? null,
				debounce_id: task.debounce?.id ?? null,
				rate_limit_id: task.rateLimit?.id ?? null,
				sleep_for: task.debounce?.s ?? null,
				throttle_duration: task.throttle?.s ?? null,
				status: task.debounce ? 'stalled' : 'pending'
			})
			if (!inserted) return [null, false, undefined]
			if (!task.debounce) return [null, true, undefined]
			const cancelled = cancelMatchingDebounceStmt.get({ debounce_id: task.debounce.id, queue: task.queue, new_id: inserted.id })
			return [null, true, cancelled]
		})

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
			timeout: number | null
			wait_for: string | null
			wait_filter: string | null
			wait_retroactive: number | null
			wait_from: number | null
			discovered_on: number | null
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
				timeout_at,
				wait_for,
				wait_filter,
				wait_from,
				discovered_on,
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
				CASE @timeout WHEN NULL THEN NULL ELSE (unixepoch('subsec') + @timeout) END,
				@wait_for,
				@wait_filter,
				CASE @wait_retroactive WHEN TRUE THEN 0 ELSE (unixepoch('subsec')) END,
				@discovered_on,
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

		/**
		 * update all steps that are waiting for an event.
		 */
		const resolveAllStepEventsStmt = this.#db.prepare<{ queue: string }, {}>(/* sql */ `
			WITH waiting_steps AS (
				SELECT
					queue, status, wait_for, wait_filter, wait_from, id
				FROM ${stepsTable} step
				WHERE step.status = 'waiting'
					AND step.queue = @queue
					AND step.wait_for IS NOT NULL
					AND step.wait_filter IS NOT NULL
			),
			base AS (
				SELECT
					step.id AS step_id,
					step.wait_for,
					step.wait_from,
					step.status,
					count(*) OVER (PARTITION BY step.id) AS count,
					filter.type,
					filter.fullKey,
					filter.value
				FROM waiting_steps step
				INNER JOIN json_tree(step.wait_filter) filter
					ON filter.type != 'null'
			),
			matches AS (
				SELECT
					base.step_id,
					event.key,
					event.created_at,
					event.queue,
					event.data
				FROM base
				LEFT JOIN ${eventsTable} event
					ON event.queue = @queue
					AND event.key = base.wait_for
					AND event.created_at >= base.wait_from
				WHERE CASE base.type
					WHEN 'object' THEN (
						json_extract(event.input, base.fullKey) IS NOT NULL
						AND json_type(json_extract(event.input, base.fullKey)) = 'object'
					)
					WHEN 'array' THEN (
						json_extract(event.input, base.fullKey) IS NOT NULL
						AND json_type(json_extract(event.input, base.fullKey)) = 'array'
					)
					ELSE (
						json_extract(event.input, base.fullKey) IS NOT NULL
						AND json_extract(event.input, base.fullKey) = base.value
					)
					END
				GROUP BY base.step_id, event.key, event.created_at, event.queue, event.data
				HAVING count(*) = base.count
			),
			results AS (
				SELECT
					step.id,
					matches.key,
					matches.data as event_data
				FROM waiting_steps step
				LEFT JOIN matches
				ON step.id = matches.step_id
			)
			UPDATE ${stepsTable}
			SET status = CASE
					WHEN results.key IS NOT NULL THEN 'completed'
					ELSE status
				END,
				data = CASE
					WHEN results.key IS NOT NULL THEN results.event_data
					ELSE data
				END,
				wait_from = unixepoch('subsec') -- update timestamp so we don't re-check past events next loop
			FROM results
			WHERE
				results.id = ${stepsTable}.id
				AND (
					results.key IS NOT NULL
					OR wait_from < unixepoch('subsec') - 0.05 -- if no event found, only update 'wait_from' in increments of 50ms minimum
				)
			RETURNING 1
		`)
	}

	#getTaskStmt!: BetterSqlite3.Statement<{ queue: string, job: string, key: string }, Task | undefined>
	#getNextTaskTx!: BetterSqlite3.Transaction<(queue: string) => [task: Task, steps: Step[], hasNext: boolean] | undefined>
	#getNextFutureTaskStmt!: BetterSqlite3.Statement<{ queue: string }, { ms: number | null }>
	#resolveTaskStmt!: BetterSqlite3.Statement<{ queue: string, job: string, key: string, status: TaskStatus, data: string | null }>
	#loopTaskStmt!: BetterSqlite3.Statement<{ queue: string, job: string, key: string }>
	#addTaskTx!: BetterSqlite3.Transaction<(task: {
		queue: string,
		job: string,
		key: string,
		input: string,
		parent_id: number | null,
		priority: number,
		timeout: number | null,
		debounce: { s: number, id: string } | null
		throttle: { s: number, id: string } | null
		rateLimit: { s: number, id: string } | null
	}) => [rateLimitMs: number | null, inserted: boolean, cancelled?: Task]>
	#recordStepStmt!: BetterSqlite3.Statement<{
		queue: string
		job: string
		key: string
		task_id: number
		step: string
		runs: number
		status: StepStatus
		next_status: StepStatus | null
		sleep_for: number | null
		timeout: number | null
		wait_for: string | null
		wait_filter: string | null
		wait_retroactive: number | null
		wait_from: number | null
		discovered_on: number | null
		data: string | null
	}>
	#recordEventStmt!: BetterSqlite3.Statement<{ queue: string, key: string, input: string, data: string }>

	getTask<T>(queue: string, job: string, key: string, cb: (task: Task | undefined) => T): T {
		const task = this.#getTaskStmt.get({ queue, job, key })
		return cb(task)
	}

	addTask<T>(task: {
		queue: string
		job: string
		key: string
		input: string
		parent_id: number | null
		priority: number
		timeout: number | null
		debounce: { s: number, id: string } | null
		throttle: { s: number, id: string } | null
		rateLimit: { s: number, id: string } | null
	}, cb?: (rateLimit: number | null, inserted: boolean, cancelled?: Task) => T): T {
		const [rateLimit, inserted, cancelled] = this.#addTaskTx.exclusive(task)
		return cb?.(rateLimit, inserted, cancelled) as T
	}

	startNextTask<T>(queue: string, cb: (result: [task: Task, steps: Step[], hasNext: boolean] | undefined) => T): T {
		const result = this.#getNextTaskTx.exclusive(queue)
		return cb(result)
	}

	nextFutureTask<T>(queue: string, cb: (result: { ms: number | null }) => T): T {
		const result = this.#getNextFutureTaskStmt.get({ queue })!
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

	recordStep<T>(task: Task, step: Pick<Step, 'step' | 'status' | 'next_status' | 'data' | 'wait_for' | 'wait_filter' | 'wait_retroactive' | 'runs' | 'discovered_on'> & {
		sleep_for?: number
		timeout?: number
	}, cb: () => T): T {
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
			timeout: step.timeout ?? null,
			wait_for: step.wait_for ?? null,
			wait_filter: step.wait_filter ?? null,
			wait_retroactive: Number(step.wait_retroactive) ?? null,
			wait_from: null,
			discovered_on: step.discovered_on,
			step: step.step
		})
		return cb()
	}

	recordEvent<T>(queue: string, key: string, input: string, data: string, cb?: () => T): T {
		this.#recordEventStmt.run({ queue, key, input, data })
		return cb?.() as T
	}

	close() {
		if (!this.#externalDb) {
			this.#db.pragma('optimize')
			this.#db.close()
		}
	}
}