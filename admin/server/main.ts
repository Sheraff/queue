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
import { format } from "prettier"

type Step = object
type Task = object
type Event = object

const foo = new Job({
	id: 'foo',
	input: z.object({
		k: z.number(),
		date: z.string().datetime(),
		parity: z.boolean().optional(),
	}),
	priority: () => Math.floor(Math.random() * 10),
}, async ({ k, parity }) => {
	const iter = await Job.run('random-iter', () => Math.ceil(Math.random() * k))
	for (let i = 0; i < iter; i++) {
		await Job.sleep(Math.random() * k * 500 + 2_000)
		await Promise.all([
			Job.run({ id: 'some-task', retry: 40, backoff: "10s" }, async () => {
				await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * k * 100))
				if (Math.random() < 0.5) {
					throw new Error("random error")
				}
				return 3
			}),
			Job.sleep("1s").then(() => Job.run('other-task', async () => {
				await new Promise(resolve => setTimeout(resolve, Math.random() * k * 50 + 500))
				return 3
			}))
		])
	}
	await Job.sleep("5s")
	if (parity) {
		Job.dispatch(foo, { k: k + 1, date: new Date().toISOString(), parity: false })
		Job.dispatch(foo, { k: k + 1, date: new Date().toISOString(), parity: true })
	}
	return 2
})

const db = new Database()

const queue = new Queue({
	id: 'my-queue',
	storage: new SQLiteStorage({ db }),
	jobs: { foo }
})

queue.jobs.foo.dispatch({ k: 0, date: new Date().toISOString(), parity: true })

const tasksStmt = db.prepare<{ queue: string, origin: number }, Task>('SELECT * FROM tasks WHERE queue = @queue AND updated_at > @origin ORDER BY created_at ASC')
const stepsStmt = db.prepare<{ queue: string, origin: number }, Step>('SELECT * FROM steps WHERE queue = @queue AND updated_at > @origin ORDER BY created_at ASC')
const eventsStmt = db.prepare<{ queue: string, origin: number }, Event>('SELECT * FROM events WHERE queue = @queue AND created_at > @origin ORDER BY created_at ASC')
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

	if (url.pathname === '/api/now') {
		res.writeHead(200, { 'Content-Type': 'text/plain' })
		res.end(String(dateStmt.get()!.date))
		return
	}

	if (url.pathname === '/api/queues') {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify([queue.id, 'fake-queue'], null, '\t'))
		return
	}

	if (url.pathname === '/api/jobs') {
		// TODO: in real conditions, we should look at the ?queue query parameter to return the appropriate jobs
		res.writeHead(200, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify(Object.keys(queue.jobs), null, '\t'))
		return
	}

	const source = url.pathname.match(/^\/api\/source\/(.+)$/)
	if (source) {
		if (!(source[1] in queue.jobs)) {
			res.writeHead(404, { 'Content-Type': 'application/json' })
			res.end(JSON.stringify({ error: 'not found' }, null, '\t'))
			return
		}
		const job = queue.jobs[source[1] as keyof typeof queue.jobs]!
		return format(job.string, { parser: "typescript", semi: false })
			.then((str) => {
				res.writeHead(200, { 'Content-Type': 'text/plain' })
				res.end(str)
			})
			.catch((error) => {
				res.writeHead(500, { 'Content-Type': 'application/json' })
				res.end(JSON.stringify({ error: error.message }, null, '\t'))
			})
	}

	const job = url.pathname.match(/^\/api\/jobs\/(.+)$/)
	if (job) {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		const id = job[1]
		const tasks = db.prepare('SELECT * FROM tasks WHERE queue = @queue AND job = @job ORDER BY created_at ASC').all({ queue: queue.id, job: id })
		res.end(JSON.stringify(tasks, null, '\t'))
		return
	}

	const singleTask = url.pathname.match(/^\/api\/task\/(.+)$/)
	if (singleTask) {
		res.writeHead(200, { 'Content-Type': 'application/json' })
		const id = singleTask[1]
		const task = db.prepare('SELECT * FROM tasks WHERE queue = @queue AND id = @id').get({ queue: queue.id, id: id })
		res.end(JSON.stringify(task, null, '\t'))
		return
	}

	const task = url.pathname.match(/^\/api\/tasks\/(.+)$/)
	if (task) {
		const id = Number(task[1])
		const data = db.prepare('SELECT * FROM tasks WHERE id = @id ORDER BY created_at ASC').get({ id })
		if (!data) {
			res.writeHead(404, { 'Content-Type': 'application/json' })
			res.end(JSON.stringify({ error: 'not found' }, null, '\t'))
			return
		}
		res.writeHead(200, { 'Content-Type': 'application/json' })

		const steps = db.prepare('SELECT * FROM steps WHERE task_id = @id ORDER BY created_at ASC').all({ id })

		const events = db.prepare('SELECT * FROM events WHERE queue = @queue AND input = @input AND (key LIKE @job OR key LIKE @step) ORDER BY created_at ASC').all({
			queue: queue.id,
			input: data.input,
			job: `job/${data.job}/%`,
			step: `step/${data.job}/%`,
		})

		const date = dateStmt.get()!.date as number

		res.end(JSON.stringify({ steps, events, date }, null, '\t'))
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