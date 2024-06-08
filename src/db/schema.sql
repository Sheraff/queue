CREATE TABLE
	IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		program TEXT NOT NULL,
		step INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending', -- pending, running, success, failure
		data TEXT NOT NULL DEFAULT '{}',
		created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		parent_id TEXT DEFAULT NULL,
		parent_key TEXT DEFAULT NULL
	);