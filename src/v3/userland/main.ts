import Database from "better-sqlite3"
import { Queue, SQLiteStorage } from "../lib"
import { bar } from "./bar"
import { foo } from "./foo"



const queue = new Queue({
	id: 'foo',
	jobs: {
		foo,
		bar,
	},
	storage: new SQLiteStorage({
		db: new Database('foo.db')
	})
})