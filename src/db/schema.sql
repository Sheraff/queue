CREATE TABLE
	IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		program TEXT NOT NULL,
		step INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending', -- pending, running, success, failure, sleeping, waiting
		retry INTEGER NOT NULL DEFAULT 0,
		concurrency INTEGER NOT NULL DEFAULT 1e999, -- 1e999 = infinity
		delay_between_seconds REAL NOT NULL DEFAULT 0,
		data TEXT NOT NULL DEFAULT '{}',
		-- parent task (child task was registered during the execution of parent task)
		parent_id TEXT DEFAULT NULL,
		parent_key TEXT DEFAULT NULL,
		-- sleep
		wakeup_at REAL DEFAULT NULL,
		-- wait for (parent task registered a listener, not knowing if/when child task will exist)
		wait_for_program TEXT DEFAULT NULL,
		wait_for_key TEXT DEFAULT NULL,
		wait_for_path TEXT DEFAULT NULL,
		wait_for_value TEXT DEFAULT NULL,
		--
		created_at REAL NOT NULL,
		updated_at REAL NOT NULL
	);