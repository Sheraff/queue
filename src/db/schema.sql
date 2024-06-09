CREATE TABLE
	IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		program TEXT NOT NULL,
		step INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending', -- pending, running, success, failure, sleeping, waiting
		data TEXT NOT NULL DEFAULT '{}',
		-- parent task
		parent_id TEXT DEFAULT NULL,
		parent_key TEXT DEFAULT NULL,
		-- sleep
		wakeup_at INTEGER DEFAULT NULL,
		-- wait for
		wait_for_program TEXT DEFAULT NULL,
		wait_for_key TEXT DEFAULT NULL,
		wait_for_path TEXT DEFAULT NULL,
		wait_for_value TEXT DEFAULT NULL,
		--
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);