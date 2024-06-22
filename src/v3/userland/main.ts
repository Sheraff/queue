import { Queue, SQLiteStorage } from "../lib"
import { bar } from "./bar"
import { foo } from "./foo"



const queue = new Queue({
	jobs: {
		foo,
		bar,
	},
	storage: new SQLiteStorage()
})