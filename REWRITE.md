---
tags: 
	- TypeScript
	- Node.js
	- Background Job
	- Task Queue
	- Asynchronous
	- Scheduler
	- Durable Execution
	- Event-Driven
	- Orchestration
	- Self-Hosted
---

# Asynchronous Task Queue System with TypeScript

This library provides a flexible and efficient task queue system for managing asynchronous jobs and workflows. It is built with TypeScript and designed to handle complex job orchestration, event-driven workflows, and durable execution of tasks. The system supports parallel execution, retries, timeouts, and customizable backoff strategies, making it suitable for a wide range of applications.

## Example

Let's say you are writing a music streaming service and you want to process user uploads.

```ts
const processFile = createJob({
	name: 'processFile',
	input: z.object({ path: z.string() }),
}, async ({ path }) => {
	const [
		{title, artist, id, palette},
		{path: transcodedPath},
	] = await Promise.all([
		metadata.run({ path }),
		transcode.run({ path }),
	])
	await run({
		name: 'store-metadata',
		concurrency: 1,
	}, async () => {
		await db.run('INSERT INTO songs VALUES (?, ?, ?, ?)', [id, title, artist, palette])
	})
	await run('notify-user', async () => {
		await sendNotification({
			title: "Song added",
			body: `${title} by ${artist} has been added to your library`
		})
	})
})

const transcode = createJob({
	name: 'transcode',
	input: z.object({ path: z.string() }),
	output: z.object({ path: z.string() }),
	concurrency: 2,
	timeout: '5m',
}, async ({ path }) => {
	const outputPath = path.replace(/\.mp3$/, '.ogg')
	await run('ffmpeg', async () => {
		await exec(`ffmpeg -i ${path} -c:a libvorbis ${outputPath}`)
	})
	return {path: outputPath}
})

const metadata = createJob({
	name: 'metadata',
	input: z.object({ path: z.string() }),
	output: z.object({
		title: z.string(),
		artist: z.string(),
		id: z.string(),
		palette: z.array(z.string()),
	}),
	concurrency: 10,
}, async ({ path }) => {
	const fingerprint = await run('fpcalc', async () => {
		return exec(`fpcalc ${path}`)
	})
	const {id} = await musicBrainz.run({ fingerprint })
	const all = await Promise.all([
		spotify.run({ id }),
		lastFm.run({ id }),
		audioDB.run({ id }),
	])
	const metadata = parseMetadata(all)
	const coverArt = await run('download-cover-art', async () => {
		const {path} = download(metadata.coverArtUrl)
		return path
	})
	const colorPalette = await thread('color-extractor', async ({ path }) => {
		return complicatedColorMaths(path)
	}, { path: coverArt })
	return {
		title: metadata.title,
		artist: metadata.artist,
		id,
		palette: colorPalette,
	}
})

const musicBrainz = createJob({
	name: 'musicBrainz',
	input: z.object({ fingerprint: z.string() }),
	output: z.object({ id: z.string() }),
	throttle: '1 per second',
}, async ({ fingerprint }) => {
	const data = await run('musicbrainz-fetch', async () => {
		const response = await fetch(`https://musicbrainz.org/ws/2/recording?query=${fingerprint}`)
		return response.json()
	})
	const id = parseMusicBrainzResponse(data)
	return {id}
})

const spotify = createJob(...)
const lastFm = createJob(...)
const audioDB = createJob(...)

const queue = createQueue({
	jobs: {
		processFile,
		transcode,
		metadata,
		musicBrainz,
		spotify,
		lastFm,
		audioDB,
	},
})

queue.jobs.processFile
	.run({ path: 'song.mp3' })
	.then(() => console.log('Job completed'))
```

In this example, we define a series of jobs that process a user-uploaded song file. The `processFile` job orchestrates the workflow by calling other jobs to extract metadata, transcode the file, and store the results in a database. Each job is defined with its input and output types, concurrency settings, and other options. The `queue` object manages the execution of these jobs and ensures that they run in the correct order with the specified constraints.

## Core Concepts

- **Queue**: A collection of jobs and pipes that orchestrates the execution of tasks.
- **Job**: A unit of work that performs a specific task or operation.
- **Pipe**: A connection between jobs that allows passing data or triggering events.

## API

- `createQueue(options: QueueOptions): Queue`: Creates a new task queue with the specified options.
	- `QueueOptions`: Configuration options for the task queue.
		- `jobs: { [K in string]: Job<K> }`: An object containing the job definitions for the queue.
		- `pipes: { [K in string]: Pipe<K> }`: An array of pipe definitions for connecting jobs.
		- `storage: Storage`: An optional storage backend for persisting job state (default: in-memory).
		- `logger: Logger`: An optional logger for recording job execution and errors.
		- `concurrency: number`: The maximum number of jobs that can run concurrently (default:	`Infinity`).
		- `cronScheduler: CronScheduler`: An optional scheduler for running jobs on a schedule (default: `node-cron` if available, none otherwise).
	- `Queue`: An object representing the task queue.
		- `jobs`: The jobs object passed to `createQueue`, but with added context allowing jobs to be run.
		- `pipes`: The pipes object passed to `createQueue`, but with added context allowing pipes to be triggered.
		- `ready: Promise<void>`: A promise that resolves when the queue is ready to start processing jobs.
		- `close(): Promise<void>`: Stops the queue and releases any resources.
		- `logger`: The logger object passed to `createQueue`, provided for convenience.

- `createJob(options: JobOptions, handler: JobHandler): Job`: Creates a new job with the specified options and handler function.
	- `JobOptions`: Configuration options for the job.
		- `name: string`: The unique name of the job.
		- `input?: StandardSchemaV1`: A validator for the job input data, follows the [standard-schema](https://github.com/standard-schema/standard-schema) format.
		- `output?: StandardSchemaV1`: A validator for the job output data, follows the [standard-schema](https://github.com/standard-schema/standard-schema) format.
		- `retry?: number | (input: any) => number`: The number of times the job should be retried if it fails, or a function that returns the number of retries based on the input data (default: `3`).
		- `debounce?`: A job with a debounce ID will be delayed until the debounce duration has passed. If another job with the same debounce ID is triggered before the duration has passed, the previous job will be canceled. Accepted configs:
			- `number | Frequency`, the debounce duration in milliseconds (e.g. `1000` or `'3/s'`). The ID will be the job name.
			- `DebounceConfig`
				- `id: string`: The debounce ID.
				- `duration: number | Frequency`: The debounce duration.
				- `mode?: 'leading' | 'trailing'`: The behavior of the debounce function (default: `trailing`).
					- in `leading` mode, the job will run immediately, and subsequent triggers within the debounce duration after this initial run will behave like the `trailing` mode.
					- in `trailing` mode, the job will be delayed by the debounce duration. If another trigger occurs within the debounce duration, the previous trigger is discarded and the debounce duration is reset. If it reaches the end of the debounce duration without another trigger, the job will run.
				- `timeout?: Duration | number`: if provided, a series of triggers that keep reseting the debounce duration will still run the job after the timeout duration has passed since the first trigger.
			- `(input: any) => number | Frequency | DebounceConfig`: A function that returns the debounce duration based on the input data.
		- `throttle?`: A job with a throttle ID will be delayed until the throttle duration has passed since the last job with the same throttle ID was triggered. Accepted configs:
			- `number | Frequency`, the throttle duration in milliseconds (e.g. `1000` or `'3/s'`). The ID will be the job name.
			- `ThrottleConfig`
				- `id: string`: The throttle ID.
				- `duration: number | Frequency`: The throttle duration.
				- `mode: 'discard' | 'queue'`: The behavior of the throttle function (default: `discard`).
					- in `discard` mode, the job will be discarded if another trigger occurs within the throttle duration.
					- in `queue` mode, the job will be queued and run after the throttle duration has passed since the last job was triggered.
			- `(input: any) => number | Frequency | ThrottleConfig`: A function that returns the throttle duration based on the input data.
		- `timeout?: Duration | number`: The maximum time the job can run before being considered failed (default: `undefined`).
		- `concurrency?`: A job with a concurrency ID will be limited to running a maximum number of instances concurrently. Accepted configs:
			- `number | Frequency`, the maximum number of instances that can run concurrently (e.g. `1` or `'3/s'` or `'2 per 5 minutes'`). The ID will be the job name.
			- `ConcurrencyConfig`
				- `id: string`: The concurrency ID.
				- `limit: number | Frequency`: The maximum number of instances that can run concurrently.
				- `mode: 'window' | 'rolling'`: The behavior of the concurrency function (default: `window`).
					- in `window` mode, the concurrency limit is applied to the total number of instances running at any given time.
					- in `rolling` mode, the concurrency limit is applied to the number of instances that start within the specified time window.
		- `backoff?`: A backoff strategy to use when retrying the job after a failure. Accepted configs:
			- `number | Duration`, a static duration between retries (e.g. `1000` (ms) or `'3s'`).
			- `BackoffConfig`
				- `initial: number | Duration`: The initial duration between retries.
				- `max?: number | Duration`: The maximum duration between retries.
				- `factor?: number`: The multiplier applied to the previous duration to calculate the next duration (default: `2`).
				- `jitter?: number`: The maximum percentage by which the duration can be randomly adjusted (default: `0`).
				- `attempts?: number`: The maximum number of retry attempts (default: `Infinity`).
			- `Array<number | Duration>`: An array of durations to use as a sequence of backoff intervals. If the job fails more times than the number of durations provided, the last duration will be used for subsequent retries.
			- `(attempts: number, input: any) => number | Duration`: A function that returns the duration between retries based on the number of attempts and input data.
		- `onTrigger(input: any)`: A function that is called when the job is triggered.
		- `onStart(input: any)`: A function that is called when the job starts running.
		- `onSuccess(input: any, output: any)`: A function that is called when the job completes successfully.
		- `onFailure(input: any, reason: any)`: A function that is called when the job fails.
		- `onError(input: any, reason: any)`: A function that is called when the job encounters an error (which might not be a failure if the job has retries).
		- `onCancel(input: any, reason: any)`: A function that is called when the job is canceled.
		- `onSettled(input: any, {cancel: any} | {failure: any} | {output: any})`: A function that is called when the job completes, fails, or is canceled.
	- `JobHandler: (input: any) => Promise<any>`: The function that performs the work of the job.
	- `Job`: An object representing the job, extends `EventEmitter`.
		- `run(input: any): Promise<any>`: Triggers the job with the specified input data, returning the output data when complete.
		- `start(input: any): void`: Triggers the job with the specified input data, without waiting for it to complete.
		- `cancel(input: any): void`: Cancels the job with the specified input data.
		- `waitFor(input: object | ((input: any) => boolean)): Promise<any>`: Waits for the specified input data to be processed by the job, returning the output data when complete (must be called inside the handler of another job).

- `createPipe(options: PipeOptions): Pipe`: Creates a new pipe with the specified options and handler function.
	- `PipeOptions`: Configuration options for the pipe.
		- `name: string`: The unique name of the pipe.
		- `input?: StandardSchemaV1`: A validator for the pipe input data, follows the [standard-schema](https://github.com/standard-schema/standard-schema) format.
	- `Pipe`: An object representing the pipe.
		- `send(input: any): Promise<void>`: Triggers the pipe with the specified input data.
		- `into(format: (input: any) => any): Pipe`: Creates a new pipe that transforms the input data before sending it.
		- `waitFor(input: object | ((input: any) => boolean)): Promise<any>`: Waits for the specified input data to be sent through the pipe, returning the output data when complete (must be called inside the handler of a job).

- `run`
	- `run(name: string, handler: () => (any | Promise<any>)): Promise<any>`
	- `run(options: RunOptions, handler: () => (any | Promise<any>)): Promise<any>`
		- `RunOptions`
			- `name?: string`: A name for this handler, must be unique within the job.
			- `concurrency?: number`: The maximum number of instances of this handler that can run concurrently.
			- `retry?: number`: The number of times the handler should be retried if it fails (default: `3`).
			- `backoff?: number | Duration | Array<number | Duration> | (attempts: number) => number | Duration`: A backoff strategy to use when retrying the handler after a failure.
			- `timeout?: Duration | number`: The maximum time the handler can run before being considered failed.
- `thread`
	- `thread(name: string, handler: (input: any, context: ThreadContext) => (any | Promise<any>), input: any): Promise<any>`
	- `thread(options: ThreadOptions, handler: (input: any, context: ThreadContext) => (any | Promise<any>), input: any): Promise<any>`
		- `ThreadOptions extends RunOptions, ResourceLimits`
- `sleep`
	- `sleep(duration: Duration | number): Promise<void>`

## Kitchen Sink

```ts
import { createQueue, createJob, createPipe, run, thread, sleep } from 'queue'

const p = createPipe({
	name: 'p',
	input: z.object({ value: z.number() }),
})

const foo = createJob({
	name: 'foo',
	input: z.object({ something: z.string() }),
	output: z.object({ something: z.string() }),
	trigger: [
		p.into(({ value }) => ({ something: value.toString() })),
	],
	throttle: {
		duration: '1/s',
		mode: 'queue',
	},
}, async ({ something }) => {
	await sleep(1000)
	console.log('foo', something)
	return { something }
})

const bar = createJob({
	name: 'bar',
	input: z.object({ other: z.number() }),
	trigger: [
		foo.into(({ something }) => ({ other: Number(something) })),
	]
}, async () => {
	await p.waitFor({ value: 55 })
	console.log('bar')
})

const baz = createJob({
	name: 'baz',
}, async () => {
	await p.send({ value: 42 })
})

const storage = createSqliteStorage({ path: 'queue.db' })

const queue1 = createQueue({
	jobs: {
		foo,
		bar,
		baz,
	},
	pipes: {
		p,
	},
	storage,
})

queue1.ready.then(() => {
	queue1.jobs.baz.run()
})

const hello = createJob({
	name: 'hello',
	input: z.object({ name: z.string() }),
	response: z.object({ response: z.string() }),
}, async ({name}) => {
	const response = await thread('world', {retry: 0}, async ({name, logger}) => {
		await sleep(1000)
		logger('world')
		if (name === 'hello') return 'world'
		throw new Error('name is not hello')
	}, {name})
	return {response}
})

const queue2 = createQueue({
	jobs: {
		hello,
	},
	storage,
})

queue2.jobs.hello.on('success', ({response}) => {
	console.log(response)
})
queue2.jobs.hello.on('failure', (error) => {
	console.error(error)
})
queue2.jobs.hello.start({name: 'hello'})
```