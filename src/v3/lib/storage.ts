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
	queue: string
	job: string
	key: string
	input: string
	status: TaskStatus
	runs: number
	created_at: number
	updated_at: number
	data: string | null
}

type StepStatus =
	/** step is a promise, currently resolving */
	| 'running'
	/** step is blocked by a timer (retry, concurrency, sleep) or event (waitFor, invoke) */
	| 'stalled'
	/** step finished, data is the result */
	| 'completed'
	/** step failed, data is the error */
	| 'failed'

export type Step = {
	queue: string
	job: string
	key: string
	/**
	 * `system/sleep#1`
	 * `user/my-id#0`
	 */
	step: string
	status: StepStatus
	runs: number
	created_at: number
	data: string | null
}

export interface Storage {
	close(): void | Promise<void>
	addTask<T>(task: { queue: string, job: string, key: string, input: string }, cb?: () => T): T | Promise<T>
	startNextTask<T>(queue: string, cb: (task: [task: Task, steps: Step[], hasNext: boolean] | undefined) => T): T | Promise<T>
	resolveTask<T>(task: Task, status: 'completed' | 'failed' | 'cancelled', data: string | null, cb?: () => T): T | Promise<T>
	requeueTask<T>(task: Task, cb: () => T): T | Promise<T>
	// TODO: shouldn't contain logic like this, just take data in
	recordStep<T>(job: string, task: Task, memo: Step | undefined, step: Pick<Step, 'step' | 'status' | 'data'>, cb: () => T): T | Promise<T>
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
				parent_id INTEGER,
				queue TEXT NOT NULL,
				job TEXT NOT NULL,
				key TEXT NOT NULL,
				input JSON NOT NULL,
				status TEXT NOT NULL,
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
				system INTEGER NOT NULL DEFAULT FALSE,
				key TEXT NOT NULL,
				step TEXT NOT NULL,
				status TEXT NOT NULL,
				runs INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				updated_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				data JSON
			);
		
			CREATE UNIQUE INDEX IF NOT EXISTS ${stepsTable}_job_key_step ON ${stepsTable} (queue, job, key, step);
			CREATE INDEX IF NOT EXISTS ${stepsTable}_task_id ON ${stepsTable} (task_id);
		
			CREATE TABLE IF NOT EXISTS ${eventsTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				queue TEXT NOT NULL,
				key TEXT NOT NULL,
				created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
				input JSON
			);
		
			CREATE INDEX IF NOT EXISTS ${eventsTable}_key ON ${eventsTable} (queue, key);
		`)

		const getNextTaskStmt = this.#db.prepare<{ queue: string }, Task>(/* sql */ `
			SELECT *
			FROM ${tasksTable}
			WHERE queue = @queue AND status = 'pending'
			ORDER BY created_at ASC
			LIMIT 2
		`)

		const reserveTaskStmt = this.#db.prepare<{ id: number }>(/* sql */ `
			UPDATE ${tasksTable}
			SET
				status = 'running',
				updated_at = unixepoch('subsec')
			WHERE id = @id
		`)

		const getTaskStepDataStmt = this.#db.prepare<{ id: number }, Step>(/* sql */ `
			SELECT *
			FROM ${stepsTable}
			WHERE
				task_id = @id
		`)

		this.#getNextTaskTx = this.#db.transaction((queue: string) => {
			const [task, next] = getNextTaskStmt.all({ queue })
			if (!task) return
			// @ts-expect-error -- id exists, but not exposed in the type
			reserveTaskStmt.run(task)
			// @ts-expect-error -- id exists, but not exposed in the type
			const steps = getTaskStepDataStmt.all(task)
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

		this.#addTaskStmt = this.#db.prepare<Task>(/* sql */ `
			INSERT INTO ${tasksTable} (queue, job, key, input, status)
			VALUES (@queue, @job, @key, @input, 'pending')
		`)

		// create or update step
		this.#recordStepStmt = this.#db.prepare<Step>(/* sql */ `
			INSERT INTO ${stepsTable} (queue, job, key, step, task_id, status, data)
			VALUES (@queue, @job, @key, @step, @task_id, @status, @data)
			ON CONFLICT (queue, job, key, step)
			DO UPDATE SET
				status = @status,
				updated_at = unixepoch('subsec'),
				data = @data
		`)

	}

	#getNextTaskTx: BetterSqlite3.Transaction<(queue: string) => [task: Task, steps: Step[], hasNext: boolean] | undefined>
	#resolveTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string, status: TaskStatus, data: string | null }>
	#loopTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string }>
	#addTaskStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string, input: string }>
	#recordStepStmt: BetterSqlite3.Statement<{ queue: string, job: string, key: string, step: string, status: StepStatus, data: string | null }>

	addTask<T>(task: { queue: string, job: string, key: string, input: string }, cb?: () => T): T {
		this.#addTaskStmt.run(task)
		return cb?.() as T
	}

	startNextTask<T>(queue: string, cb: (result: [task: Task, steps: Step[], hasNext: boolean] | undefined) => T): T {
		const result = this.#getNextTaskTx.exclusive(queue)
		return cb(result)
	}

	resolveTask<T>(task: Task, status: "completed" | "failed" | "cancelled", data: string | null, cb?: () => T): T {
		this.#resolveTaskStmt.run({ queue: task.queue, job: task.job, key: task.key, status, data })
		return cb?.() as T
	}

	requeueTask<T>(task: Task, cb: () => T): T {
		this.#loopTaskStmt.run({ queue: task.queue, job: task.job, key: task.key })
		return cb?.() as T
	}

	recordStep<T>(job: string, task: Task, memo: Step | undefined, step: Pick<Step, 'step' | 'status' | 'data'>, cb: () => T): T {
		this.#recordStepStmt.run({
			queue: task.queue,
			job,
			key: task.key,
			// @ts-expect-error -- id exists, but not exposed in the type
			task_id: task.id,
			data: step.data,
			status: step.status,
			step: step.step
		})
		return cb()
	}

	close() {
		if (!this.#externalDb)
			this.#db.close()
	}
}