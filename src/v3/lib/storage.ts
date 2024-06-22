import BetterSqlite3 from "better-sqlite3"

type Task = {
	queue: string
	job: string
	key: string
	input: string
	status: string
	runs: number
	created_at: number
	data: string | null
}

type Step = {
	queue: string
	job: string
	key: string
	step: string
	status: string
	runs: number
	created_at: number
	data: string | null
}

export interface Storage {
	close(): void | Promise<void>
	startNextTask(queue: string, cb: (task: [task: Task, steps: Step[]] | undefined) => void): void
}

export class SQLiteStorage implements Storage {
	#db: BetterSqlite3.Database

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
		} else {
			this.#db = db
		}

		const tasksTable = typeof tables === 'object' ? tables.tasks : tables ? `${tables}_tasks` : 'tasks'
		const stepsTable = typeof tables === 'object' ? tables.steps : tables ? `${tables}_steps` : 'steps'
		const eventsTable = typeof tables === 'object' ? tables.events : tables ? `${tables}_events` : 'events'
		this.#db.exec(/* sql */ `
			CREATE TABLE IF NOT EXISTS ${tasksTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				queue TEXT NOT NULL,
				job TEXT NOT NULL,
				key TEXT NOT NULL,
				input JSON NOT NULL,
				status TEXT NOT NULL,
				runs INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
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
				runs INTEGER NOT NULL DEFAULT 0,
				created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
				data JSON
			);
		
			CREATE UNIQUE INDEX IF NOT EXISTS ${stepsTable}_job_key_step ON ${stepsTable} (queue, job, key, step);
			CREATE INDEX IF NOT EXISTS ${stepsTable}_task_id ON ${stepsTable} (task_id);
		
			CREATE TABLE IF NOT EXISTS ${eventsTable} (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				queue TEXT NOT NULL,
				key TEXT NOT NULL,
				created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
				input JSON
			);
		
			CREATE INDEX IF NOT EXISTS ${eventsTable}_key ON ${eventsTable} (queue, key);
		`)

		this.#getNextTaskStmt = this.#db.prepare<{ queue: string }, Task>(/* sql */ `
			SELECT *
			FROM ${tasksTable}
			WHERE
				queue = @queue
				AND status = 'waiting'
			ORDER BY created_at ASC
			LIMIT 1
		`)

		this.#getTaskStepDataStmt = this.#db.prepare<{ id: number }, Step>(/* sql */ `
			SELECT *
			FROM ${stepsTable}
			WHERE
				task_id = @id
		`)
	}

	#getNextTaskStmt: BetterSqlite3.Statement<{ queue: string }, Task>
	#getTaskStepDataStmt: BetterSqlite3.Statement<{ id: number }, Step>

	startNextTask(queue: string, cb: (result: [task: Task, steps: Step[]] | undefined) => void) {
		// TODO: wip
		const tx = this.#db.transaction(() => {
			const task = this.#getNextTaskStmt.get({ queue })
			if (!task) return
			// TODO: update task status to 'running'
			const steps = this.#getTaskStepDataStmt.all({
				// @ts-expect-error -- id exists, but not exposed in the type
				id: task.id
			})
			return [task, steps] as [Task, Step[]]
		})
		const result = tx.exclusive()
		cb(result)
	}

	close() {
		this.#db.close()
	}
}