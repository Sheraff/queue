import Database from "better-sqlite3"
import { Queue, SQLiteStorage } from "../lib"
import { bar } from "./bar"
import { foo, fooBarPipe, otherPipe } from "./foo"



const queue = new Queue({
	id: 'foo',
	jobs: {
		foo,
		bar,
	},
	pipes: {
		fooBarPipe,
		otherPipe,
	},
	storage: new SQLiteStorage({
		db: new Database('foo.db')
	})
})