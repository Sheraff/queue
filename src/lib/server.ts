// Import the necessary module using ES Module syntax
import http from 'http'
import { Queue } from "./queue"
import { SQLiteStorage, type Step, type Task, type Event } from "./storage"
import { Job } from "./job"
import Database from "better-sqlite3"
import { z } from "zod"


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

	if (url.pathname === '/api') {
		const param = url.searchParams.get('cursor')
		const cursor = param ? Number(atob(param)) : 0
		const data = getData(queue, cursor)
		res.writeHead(200, { 'Content-Type': 'application/json' })
		res.end(JSON.stringify(data, null, '\t'))
		return
	}

	if (url.pathname === '/') {
		const data = getData(queue, 0)
		res.writeHead(200, { 'Content-Type': 'text/html' })
		res.end(`
			<!DOCTYPE html>
			<title>Queue</title>
			<h1>Queue</h1>
			<pre id="data">${JSON.stringify(data, null, '\t')}</pre>
			<button id="refresh">Refresh</button>
			<script>
				const data = ${JSON.stringify(data)}
				const pre = document.getElementById('data')
				const fetchData = async (cursor) => {
					const res = await fetch(cursor ? '/api?cursor=' + cursor : '/api')
					const json = await res.json()
					data.events.push(...json.events)
					data.jobs = json.jobs
					data.pipes = json.pipes
					data.date = json.date
					data.cursor = json.cursor
					data.tasks = json.tasks.reduce((t, cv) => {
						const task = t.find(t => t.id === cv.id)
						if (task) {
							Object.assign(task, cv)
						} else {
							t.push(cv)
						}
						return t
					}, data.tasks)
					data.steps = json.steps.reduce((t, cv) => {
						const step = t.find(t => t.id === cv.id)
						if (step) {
							Object.assign(step, cv)
						} else {
							t.push(cv)
						}
						return t
					}, data.steps)
					pre.innerText = JSON.stringify(data, null, '\t')
					return json.cursor
				}
				let cursor = '${data.cursor}'
				document.getElementById('refresh').addEventListener('click', async () => {
					cursor = await fetchData(cursor)
				})
			</script>
		`)
		return
	}

	console.log(url.pathname)

})

// Define the port to listen on
const PORT = 3000

// Start the server
server.listen(PORT, () => {
	console.log(`Server running on http://localhost:${PORT}/`)
})