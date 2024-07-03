// Import the necessary module using ES Module syntax
import http from 'http'
import {
	Queue,
	SQLiteStorage,
	// type Step, type Task, type Event, 
	Job
} from "queue"
import Database from "better-sqlite3"
import { z } from "zod"

type Step = object
type Task = object
type Event = object


const foo = new Job({
	id: 'foo',
	input: z.object({ k: z.number() }),
}, async ({ k }) => {
	for (let i = 0; i < 10; i++) {
		await Job.sleep("1s")
		const a = await Job.run('some-task', async () => 3)
	}
	Job.dispatch(foo, { k: k + 1 })
	return 2
})

const db = new Database()

const queue = new Queue({
	id: 'my-queue',
	storage: new SQLiteStorage({ db }),
	jobs: { foo }
})

queue.jobs.foo.dispatch({ k: 0 })

const tasksStmt = db.prepare<{ queue: string, origin: number }, Task>('SELECT * FROM tasks WHERE queue = @queue AND updated_at > @origin')
const stepsStmt = db.prepare<{ queue: string, origin: number }, Step>('SELECT * FROM steps WHERE queue = @queue AND updated_at > @origin')
const eventsStmt = db.prepare<{ queue: string, origin: number }, Event>('SELECT * FROM events WHERE queue = @queue AND created_at > @origin')
const dateStmt = db.prepare<[], { date: number }>("SELECT (unixepoch('subsec')) date")

const getData = (queue: Queue, origin: number) => {
	const date = dateStmt.get()!.date as number
	const tasks = tasksStmt.all({ queue: queue.id, origin }) as Task[]
	const steps = stepsStmt.all({ queue: queue.id, origin }) as Step[]
	const events = eventsStmt.all({ queue: queue.id, origin }) as Event[]
	const jobs = Array.from(Object.keys(queue.jobs))
	const pipes = Array.from(Object.keys(queue.pipes))
	const data = { tasks, steps, events, jobs, pipes, date, cursor: btoa(String(date)) }
	return data
}


// Create an HTTP server
const server = http.createServer((req, res) => {
	const url = new URL(req.url || '', `http://${req.headers.host}`)
	if (!url.pathname.startsWith('/api')) {
		res.writeHead(404)
		res.end()
		return
	}

	if (url.pathname === '/api/jobs') {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify(Object.keys(queue.jobs), null, '\t'))
		return
	}

	const job = url.pathname.match(/^\/api\/jobs\/(.+)$/)
	if (job) {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		const id = job[1]
		const tasks = db.prepare('SELECT * FROM tasks WHERE queue = @queue AND job = @job').all({ queue: queue.id, job: id })
		res.end(JSON.stringify(tasks, null, '\t'))
		return
	}

	const task = url.pathname.match(/^\/api\/tasks\/(.+)$/)
	if (task) {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		const id = Number(task[1])
		const steps = db.prepare('SELECT * FROM steps WHERE task_id = @id').all({ id })
		res.end(JSON.stringify({ steps }, null, '\t'))
		return
	}

	const param = url.searchParams.get('cursor')
	const cursor = param ? Number(atob(param)) : 0
	const data = getData(queue, cursor)
	res.writeHead(200, { 'Content-Type': 'application/json' })
	res.end(JSON.stringify(data, null, '\t'))
	return
})

// Define the port to listen on
const PORT = 3001

// Start the server
server.listen(PORT, () => {
	console.log(`Server running on http://localhost:${PORT}/`)
})