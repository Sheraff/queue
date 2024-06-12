import Database from 'better-sqlite3'

export function makeDb(filename?: string) {
	const db: Database.Database = new Database(filename, {})
	db.pragma('journal_mode = WAL')
	db.exec(/* sql */ `
		CREATE TABLE IF NOT EXISTS tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			program TEXT NOT NULL,
			key TEXT NOT NULL, -- hashed input
			input TEXT NOT NULL, -- json input
			-- pending: triggered, nothing happened yet
			-- started: picked up by a worker
			-- stalled: in progress, waiting for a step promise to resolve (resolved by JS runtime)
			-- waiting: in progress, waiting for an event to occur (resolved by SQL)
			-- cancelled: not finished, data will be a reason (timeout, debounce, event, ...)
			-- error: not finished, data will be a serialized error
			-- success: finished, data will be the output
			status TEXT NOT NULL,
			data TEXT -- { data: } json of output / error / reason (based on status)
		);
	
		CREATE UNIQUE INDEX IF NOT EXISTS tasks_program_key ON tasks (program, key);
	
		CREATE TABLE IF NOT EXISTS memo (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			program TEXT NOT NULL,
			key TEXT NOT NULL, -- hashed input
			step TEXT NOT NULL, -- step name
			-- started: execution started
			-- success: execution finished
			-- error: execution failed
			status TEXT NOT NULL,
			data TEXT -- { data: } json of output / error (based on status)
		);
	
		CREATE INDEX IF NOT EXISTS memo_program_key ON memo (program, key);
		CREATE UNIQUE INDEX IF NOT EXISTS memo_program_key_step ON memo (program, key, step);
	`)

	///////// TASK

	const insertOrReplaceTaskStatement = db.prepare(/* sql */`
		INSERT OR REPLACE
		INTO tasks (program, key, input, status, data)
		VALUES (@program, @key, @input, @status, @data)
	`)
	const insertOrReplaceTaskNoDataStatement = db.prepare(/* sql */`
		INSERT OR REPLACE
		INTO tasks (program, key, input, status)
		VALUES (@program, @key, @input, @status)
	`)

	function insertOrReplaceTask(task: {
		program: string,
		key: string,
		input: string,
		status: string,
		data?: string,
	}) {
		if (task.data) {
			insertOrReplaceTaskStatement.run(task)
		} else {
			insertOrReplaceTaskNoDataStatement.run(task)
		}
	}

	const insertOrIgnoreTaskStatement = db.prepare(/* sql */`
		INSERT OR IGNORE
		INTO tasks (program, key, input, status)
		VALUES (@program, @key, @input, @status)
	`)

	function insertOrIgnoreTask(task: {
		program: string,
		key: string,
		input: string,
		status: string,
	}) {
		insertOrIgnoreTaskStatement.run(task)
	}

	type Task = {
		program: string
		key: string
		input: string
		status: string
		data: string | null
	}
	const task = db.prepare<[], Task>(/* sql */`
		SELECT * FROM tasks
		WHERE status NOT IN ('cancelled', 'error', 'success')
		ORDER BY id
		LIMIT 1
	`)

	function getNextTask() {
		return task.get()
	}


	/////// MEMO


	const insertOrReplaceMemoStatement = db.prepare(/* sql */`
		INSERT OR REPLACE
		INTO memo (program, key, step, status, data)
		VALUES (@program, @key, @step, @status, @data)
	`)

	function insertOrReplaceMemo(memo: {
		program: string,
		key: string,
		step: string,
		status: string,
		data: string,
	}) {
		insertOrReplaceMemoStatement.run(memo)
	}

	const stepData = db.prepare<{
		program: string
		key: string
	}, {
		program: string
		key: string
		step: string
		status: string
		data: string | null
	}>(/* sql */`
		SELECT * FROM memo
		WHERE program = @program
		AND key = @key
	`)

	function getMemosForTask(task: { program: string, key: string }) {
		return stepData.all(task)
	}

	return {
		close: () => { db.close() },
		insertOrReplaceTask,
		insertOrIgnoreTask,
		getNextTask,
		insertOrReplaceMemo,
		getMemosForTask,
	}
}

export type Storage = ReturnType<typeof makeDb>