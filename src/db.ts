import Database from 'better-sqlite3'

type Task = {
	program: string
	key: string
	input: string
	status: string
	created_at: number
	timeout_at: number
	did_timeout: 0 | 1,
	debounce_group: string | null
	throttle_group: string | null
	concurrency_group: string | null
	concurrency_limit: number
	priority: number
	data: string | null
}

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
			-- waiting: { until: timestamp }
			status_data TEXT, -- extra data for status, shape depends on status
			created_at INTEGER NOT NULL DEFAULT (unixepoch('subsec')),
			settled_at INTEGER, -- timestamp of when the task was settled
			timeout_at INTEGER NOT NULL DEFAULT 1e999,
			debounce_group TEXT, -- if set, incoming tasks with the same group will reset the start timeout, task only starts after timeout
			throttle_group TEXT, -- if set, incoming tasks with the same group will be ignored for the throttle period
			concurrency_group TEXT, -- if set, only N tasks in the group can run at a time
			concurrency_limit INTEGER NOT NULL DEFAULT 1, -- sets the limit N for the concurrency group
			priority INTEGER NOT NULL,
			data TEXT -- { data: } json of output / error / reason (based on status)
		);
	
		CREATE UNIQUE INDEX IF NOT EXISTS tasks_program_key ON tasks (program, key);
		CREATE INDEX IF NOT EXISTS tasks_status ON tasks (status);
	
		CREATE TABLE IF NOT EXISTS memo (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			program TEXT NOT NULL,
			key TEXT NOT NULL, -- hashed input
			step TEXT NOT NULL, -- step name
			-- success: execution finished
			-- error: execution failed
			status TEXT NOT NULL,
			runs INTEGER NOT NULL DEFAULT 0, -- number of times this step has been run
			last_run INTEGER NOT NULL, -- timestamp of last run
			data TEXT -- { data: } json of output / error (based on status)
		);
	
		CREATE INDEX IF NOT EXISTS memo_program_key ON memo (program, key);
		CREATE UNIQUE INDEX IF NOT EXISTS memo_program_key_step ON memo (program, key, step);
	`)

	///////// TASK

	const updateTaskProgressStatement = db.prepare(/* sql */`
		UPDATE tasks
		SET
			status = @status
		WHERE
			program = @program
			AND key = @key
	`)
	function updateTaskProgress(task: {
		program: string,
		key: string,
		status: 'started' | 'stalled' | 'waiting',
	}) {
		updateTaskProgressStatement.run(task)
	}

	const settleTaskStatement = db.prepare(/* sql */`
		UPDATE tasks
		SET
			status = @status,
			settled_at = unixepoch('subsec'),
			data = @data
		WHERE
			program = @program
			AND key = @key
			AND status NOT IN ('cancelled', 'error', 'success')
	`)
	function settleTask(task: {
		program: string,
		key: string,
		status: 'cancelled' | 'error' | 'success',
		data: string | null,
	}) {
		settleTaskStatement.run(task)
	}

	const clearTaskDebounceGroupStatement = db.prepare(/* sql */`
		UPDATE tasks
		SET
			debounce_group = '---'
		WHERE
			program = @program
			AND key = @key
	`)

	function clearTaskDebounceGroup(task: {
		program: string,
		key: string,
	}) {
		clearTaskDebounceGroupStatement.run(task)
	}

	const sleepOrIgnoreTaskStatement = db.prepare(/* sql */`
		UPDATE tasks
		SET
			status = 'waiting',
			status_data = json_object('until', unixepoch('subsec') + @seconds)
		WHERE
			program = @program
			AND key = @key
	`)

	function sleepOrIgnoreTask(task: {
		program: string,
		key: string,
		seconds: number,
	}) {
		sleepOrIgnoreTaskStatement.run(task)
	}

	type NewTask = {
		program: string,
		key: string,
		input: string,
		status: 'pending',
		priority: number,
		timeout_in: number,
		debounce_group: string | null,
		throttle_group: string | null,
		concurrency_group: string | null,
		concurrency_limit: number | null,
	}

	const createTaskStatement = db.prepare<
		NewTask,
		undefined | 1
	>(/* sql */`
		INSERT OR IGNORE
		INTO tasks (program, key, input, status, priority, timeout_at, debounce_group, throttle_group, concurrency_group, concurrency_limit)
		VALUES (@program, @key, @input, @status, @priority, unixepoch('subsec') + @timeout_in, @debounce_group, @throttle_group, @concurrency_group, @concurrency_limit)
		RETURNING 1
	`)

	function createTask(task: NewTask) {
		return !!createTaskStatement.get(task)
	}


	const nextTask = db.prepare<[], Task>(/* sql */`
		SELECT *, timeout_at <= unixepoch('subsec') as did_timeout FROM tasks
		WHERE
			(
				status IS 'started'
			)
			OR (
				status IS 'waiting'
				AND json_extract(status_data, '$.until') <= unixepoch('subsec')
			)
			OR (
				status IS 'pending'
				AND (
					concurrency_group IS NULL
					OR concurrency_limit > (
						SELECT COUNT(*)
						FROM tasks
						WHERE
							status NOT IN ('cancelled', 'error', 'success', 'pending')
							AND concurrency_group = tasks.concurrency_group
					)
				)
			)
		ORDER BY
			priority DESC,
			created_at ASC,
			id
		LIMIT 2
	`)

	function getNextTask() {
		return nextTask.all()
	}

	const futureTask = db.prepare<[], { wait_seconds: number }>(/* sql */`
		SELECT
			MIN(
				json_extract(status_data, '$.until') - unixepoch('subsec'),
				timeout_at - unixepoch('subsec')
			) AS wait_seconds
		FROM tasks
		WHERE
			(
				status IS 'waiting'
				AND json_extract(status_data, '$.until') >= unixepoch('subsec')
			)
			OR (
				status NOT IN ('cancelled', 'error', 'success')
				AND timeout_at IS NOT NULL
				AND timeout_at < 1e999
			)
		ORDER BY wait_seconds ASC
		LIMIT 1
	`)
	function getNextFutureTask() {
		return futureTask.get()
	}

	const taskByKey = db.prepare<{
		program: string
		key: string
	}, Task>(/* sql */`
		SELECT * FROM tasks
		WHERE program = @program
		AND key = @key
	`)
	function getTask(task: { program: string, key: string }) {
		return taskByKey.get(task)
	}

	const taskByDebounceGroup = db.prepare<{
		debounce_group: string
	}, Task>(/* sql */`
		SELECT * FROM tasks
		WHERE
			debounce_group = @debounce_group
			AND status NOT IN ('cancelled', 'error', 'success')
	`)
	function getTaskByDebounceGroup(task: { debounce_group: string }) {
		return taskByDebounceGroup.all(task)
	}

	const latestTaskByThrottleGroup = db.prepare<{
		throttle_group: string
		timeout: number
	}, Task>(/* sql */`
		SELECT * FROM tasks
		WHERE
			throttle_group = @throttle_group
			AND status IS NOT 'cancelled'
			AND created_at + @timeout >= unixepoch('subsec')
		ORDER BY created_at DESC
		LIMIT 1
	`)
	function getLatestTaskByThrottleGroup(task: {
		throttle_group: string
		timeout: number
	}) {
		return latestTaskByThrottleGroup.get(task)
	}

	const latestSettledTaskByConcurrencyGroup = db.prepare<{
		concurrency_group: string
	}, { since: number }>(/* sql */`
		SELECT (unixepoch('subsec') - settled_at) as since FROM tasks
		WHERE
			concurrency_group = @concurrency_group
			AND settled_at NOT NULL
		ORDER BY settled_at DESC
		LIMIT 1
		OFFSET @concurrency_limit
	`)
	function getLatestSettledTaskByConcurrencyGroup(task: {
		concurrency_group: string
		concurrency_limit: number
	}) {
		return latestSettledTaskByConcurrencyGroup.get(task)
	}


	/////// MEMO


	const insertOrReplaceMemoStatement = db.prepare(/* sql */`
		INSERT OR REPLACE
		INTO memo (program, key, step, status, runs, last_run, data)
		VALUES (@program, @key, @step, @status, @runs, unixepoch('subsec'), @data)
	`)

	function insertOrReplaceMemo(memo: {
		program: string
		key: string
		step: string
		status: string
		runs: number
		data: string
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
		status: 'success' | 'error'
		runs: number
		last_run: number
		elapsed: number
		data: string | null
	}>(/* sql */`
		SELECT *, (unixepoch('subsec') - last_run) as elapsed FROM memo
		WHERE program = @program
		AND key = @key
	`)

	function getMemosForTask(task: { program: string, key: string }) {
		return stepData.all(task)
	}

	return {
		close: () => { db.close() },
		updateTaskProgress,
		settleTask,
		createTask,
		sleepOrIgnoreTask,
		clearTaskDebounceGroup,
		getNextTask,
		getNextFutureTask,
		getTask,
		getTaskByDebounceGroup,
		getLatestTaskByThrottleGroup,
		getLatestSettledTaskByConcurrencyGroup,
		insertOrReplaceMemo,
		getMemosForTask,
	}
}

export type Storage = ReturnType<typeof makeDb>
export type { Task }