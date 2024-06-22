import Database from "better-sqlite3"

export type Task = {
	program: string
	key: string
	status: string
	runs: number
	created_at: number
	data: string | null
}

export interface Storage {
	close(): void | Promise<void>
	getNextTask(cb: (task: Task | undefined) => void): void
}

const createSqlite = /* sql */ `
	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		program TEXT NOT NULL,
		key TEXT NOT NULL,
		status TEXT NOT NULL,
		runs INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
		data JSON
	);

	CREATE UNIQUE INDEX IF NOT EXISTS tasks_program_key ON tasks (program, key);

	CREATE TABLE IF NOT EXISTS steps (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		program TEXT NOT NULL,
		key TEXT NOT NULL,
		step TEXT NOT NULL,
		status TEXT NOT NULL,
		runs INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
		data JSON
	);

	CREATE UNIQUE INDEX IF NOT EXISTS steps_program_key_step ON steps (program, key, step);

	CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		key TEXT NOT NULL,
		created_at INTEGER NOT NULL DEFAULT unixepoch('subsec'),
		input JSON
	);

	CREATE INDEX IF NOT EXISTS events_key ON events (key);
`

export class SQLiteStorage implements Storage {
	#db: Database.Database

	constructor({
		db
	}: { db?: Database.Database | string }) {
		if (!db || typeof db === 'string') {
			this.#db = new Database(db)
			this.#db.pragma('journal_mode = WAL')
		} else {
			this.#db = db
		}
		this.#db.exec(createSqlite)
	}

	getNextTask(cb: (task: Task | undefined) => void) {
		// TODO: wip
		const stmt = this.#db.prepare<[], Task>(/* sql */ `
			SELECT *
			FROM tasks
			WHERE status = 'waiting'
			ORDER BY created_at ASC
			LIMIT 1
		`)
		const task = stmt.get()
		cb(task)
	}

	close() {
		this.#db.close()
	}
}