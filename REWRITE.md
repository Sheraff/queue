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

This library provides a flexible and efficient task queue system to manage asynchronous jobs and workflows. It is built firmly in TypeScript and is designed to handle complex job orchestration, event-driven workflows, and durable execution of tasks. The system supports parallel execution, retries, timeouts, and customizable backoff strategies and is extended with capabilities for observability and lifecycle management, making it suitable for a wide range of production applications.


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

## Persistence and Durability

The system provides built-in adapters for common persistence mechanisms:
- **SQLite**: For lightweight file-based storage.
- **PostgreSQL**: For robust relational persistence.
- **In-Memory**: For development and testing.
- **Redis**: For a fast, key-value store.

Each adapter implements a common interface so that additional storage mechanisms can be added by conforming to that same interface.

### Example

Creating a queue with a SQLite storage adapter:
```ts
const storage = createSqliteStorage({ path: 'queue.db' });
const queue = createQueue({
    jobs: { /* job definitions */ },
    pipes: { /* pipe definitions */ },
    storage, // Using built-in SQLite adapter
});
```

## Workflow Composition & Complexity

While basic chaining via triggers (using `into` and `trigger`) can create conditional branches, fan-out/in, split/merge, iterative workflows, and error compensation flows, a few higher-level constructs may help structure complex workflows:

### Composite Workflows

You can group a series of jobs into a composite workflow unit. This unit behaves as a single job from an orchestration standpoint—allowing you to pause, resume, and monitor the entire group as a coherent whole.

```ts
// Example composite workflow: composing several jobs as a single workflow unit
async function compositeWorkflow(input: any) {
    // Start jobA, then run jobB and jobC in parallel, aggregate results and continue
    const resultA = await jobA.run(input);
    const [resultB, resultC] = await Promise.all([
        jobB.run(resultA),
        jobC.run(resultA)
    ]);
    return { ...resultA, resultB, resultC };
}
```


## Core Concepts

- **Queue**: A collection of jobs and pipes that orchestrates the execution of tasks.
- **Job**: A unit of work that performs a specific task or operation.
- **Pipe**: A connection between jobs that allows passing data or triggering events.
- **Steps**: A series of built-in methods that can be called from inside a job, in between which the job can be paused and resumed.

## Steps

Steps are a series of built-in methods that can be called from inside a job. Each step also serves as a checkpoint, allowing the job to be paused and resumed at the point where it was paused. After each step, the state of the job is persisted to the storage adapter, and the job can be resumed from the same point, by re-hydrating the job from the storage adapter. 

- `run(name: string, handler: () => (any | Promise<any>))`: Runs a handler function, optionally with a name.
- `thread(name: string, handler: (input: any, context: ThreadContext) => (any | Promise<any>), input: any)`: Runs a handler function, optionally with a name.
- `sleep(duration: Duration | number): Promise<void>`: Pauses the job for the specified duration.
- `somePipe.send(input: any): Promise<void>`: Sends the job's output to a pipe.
- `somePipe.waitFor(input: object | ((input: any) => boolean)): Promise<any>`: Waits for the specified input data to be processed by the pipe, returning the output data when complete (must be called inside the handler of a job).
- `someJob.start(input: any): void`: Starts the job with the specified input data, without waiting for it to complete.
- `someJob.run(input: any): Promise<any>`: Runs the job with the specified input data, returning the output data when complete.
- `someJob.cancel(input: any): void`: Cancels the job with the specified input data.
- `someJob.pause(): void`: Pauses the job.
- `someJob.resume(): void`: Resumes the job.
- `someJob.waitFor(input: object | ((input: any) => boolean)): Promise<any>`: Waits for the specified input data to be processed by the job, returning the output data when complete (must be called inside the handler of another job).

### Example

```ts
const job = createJob({
	name: 'job',
}, async ({}) => {
	const data = await run('step1', async () => {
		return 'hello'
	})
	// <- here the job could be paused, either explicitly by calling `pause()` or by the orchestration server if a higher priority job is started
	await sleep(1000)
	return data
})
```

## Job Lifecycle Management and Execution Policies

Jobs are externally controllable and expose lifecycle methods. Each job execution returns a unique ID so that it can be managed programmatically or through the observability dashboard.

### Recommended Best Practices for Execution Policies

**Retries & Backoff:**  
- Use the default of 3 retries with exponential backoff in scenarios where errors may be intermittent.
- For long-running jobs or operations prone to network timeouts, consider increasing the retry count and/or applying a fixed backoff component.

**Timeouts:**  
- For rapid, predictable tasks, a short timeout (e.g., 30 seconds) is typically sufficient.
- For operations with variable execution times, avoid strict timeouts or use a generous timeout setting.

**Example: Customizing Retries and Timeout**

```ts
const customJob = createJob({
  name: 'customJob',
  input: z.object({ id: z.number() }),
  retry: 5, // Increase the number of retries
  backoff: (attempt) => Math.min(1000 * Math.pow(2, attempt), 10000), // Exponential backoff capped at 10 seconds
  timeout: 15000, // 15 seconds timeout
}, async ({ id }) => {
  // job logic here...
});
```

### Available Lifecycle Methods and Execution Policies

- `pause()`: Temporarily suspend the job.
- `resume()`: Resume a paused job.
- `cancel()`: Cancel an executing or paused job.

### Example API Usage

```ts
// Running a job and managing its lifecycle
const execution = someJob.start({ id: 'input' });
console.log(`Job started with ID: ${execution.id}`);

// Pause the job via the instance method
execution.pause();

// Later, resume the job
execution.resume();

// Or cancel the job
execution.cancel();
```

Jobs can also be controlled via the web dashboard, which exposes buttons (Pause, Resume, Cancel) that call the underlying API methods.

It is possible to retrieve a previously started job object from the queue using either the job ID, or the provided queries:

```ts
const job = await queue.jobs.get('job-1234') // by job ID, user is responsible for having a valid job ID
const jobs = await queue.jobs.getAll({ 
	startedBefore: '2025-01-01',
	startedAfter: '2024-01-01',
	completedBefore: '2025-01-01',
	completedAfter: '2024-01-01',
	retryCount: 3,
	retryCountLessThan: 3,
	retryCountGreaterThan: 3,
	jobName: 'foo',
	jobName: ['foo', 'bar'],
	status: 'running', // 'running', 'completed', 'failed', 'cancelled', 'pending', 'paused'
})
const job = await queue.jobs.getFirst({ ... })
```

---

## Extensibility and Sensible Defaults

Most advanced options have sensible defaults to ease development. Users usually won't need to configure much, but all options are available for fine-tuning when needed.

### Default Values

- **Retries**: Defaults to 3 attempts using an exponential backoff starting at 1–2 seconds.
- **Timeout**: Default timeout is "1 day".
- **Concurrency**: Defaults to `Infinity` (i.e., no limit) if not set.
- **Debounce/Throttle**: Disabled by default; they must be explicitly configured.
- **Validation**: Schema validation using Standard Schema (e.g. `zod`) is optional but recommended.

### Global (Queue-Level) Hooks

In addition to per-job hooks (`onSuccess`, `onFailure`, etc.), the Queue object itself can support global hooks that apply to all jobs. This allows users to implement cross-cutting concerns such as logging or instrumentation without repeating code on every job.

```ts
const queue = createQueue({
  jobs: { /* job definitions */ },
  pipes: { /* pipe definitions */ },
  storage: createSqliteStorage({ path: 'queue.db' }),
});

// Configure global hooks on the queue for logging every job start and completion.
queue.on('jobStart', ({ jobId, jobName, timestamp }) => {
  console.log(`[${timestamp}] Job ${jobName} (ID: ${jobId}) started.`);
});

queue.on('jobSuccess', ({ jobId, jobName, result, timestamp }) => {
  console.log(`[${timestamp}] Job ${jobName} (ID: ${jobId}) succeeded with result:`, result);
});

queue.on('jobFailure', ({ jobId, jobName, error, timestamp }) => {
  console.error(`[${timestamp}] Job ${jobName} (ID: ${jobId}) failed:`, error);
});
```

These global hooks work alongside per-job hooks to provide a comprehensive middleware approach.

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



## Inter-Job Communication and Data Flow


The API currently provides:

- **Pipes:** for one-way communication between jobs.
- **Triggers (`into`):** to map job outputs into the inputs of subsequent jobs.

Data transformation (e.g. mapping or key conversion) and bidirectional communication (awaiting a job's result, handling errors, etc.) are intended to be handled in user-land. For instance, you can simply call:

```ts
try {
  const result = await someJob.run({ data: 'input' });
  // Transform or utilize the result as needed
} catch (error) {
  console.error('Job failed', error);
}
```

This approach keeps the API lean and allows users full control over how data is transformed between jobs.

### Example

```ts
// Defining a pipe that sends data from one job to another
const eventPipe = createPipe({
    name: 'eventPipe',
    input: z.object({ eventData: z.string() }),
});
const genericPipe = createPipe({
    name: 'genericPipe',
    input: z.object({ foo: z.object({
		bar: z.string(),
	})}),
});

const listenerJob = createJob({
    name: 'listenerJob',
    output: z.object({ eventData: z.string() }),
	trigger: [
		eventPipe,
		genericPipe.into(({foo}) => ({eventData: foo.bar})),
	]
}, async ({eventData}) => {
    console.log('Received event:', event.eventData);
    return event;
});

const triggerJob = createJob({
    name: 'triggerJob',
    input: z.object({ message: z.string() }),
}, async ({ message }) => {
    await eventPipe.send({ eventData: message });
	await genericPipe.send({ foo: { bar: message } });
});
```

---

## Deployment and Scalability Considerations

The orchestration node runs centrally, acting as the source of truth for job execution state and workflows. Distributed workers execute the jobs on user-provided infrastructure. This separation makes it easy to scale:
  
- **Central Orchestration Node**: Maintains all state, locks, and the event log.
- **Distributed Execution**: Workers simply report back to the orchestration node via provided HTTP endpoints.

### Example Integration

```ts
const queue = createQueue({
	jobs: { /* job definitions */ },
    pipes: { /* pipe definitions */ },
    storage: createPostgreStorage({ connectionString: 'postgres://...' }),
	orchestration: {
		port: 4000,
		host: '0.0.0.0',
		endpoint: '/jobs',
		interval: '1/s',
	}
});

// On the orchestration node:
queue.orchestration.start()

// On any worker:
queue.worker.start({
	port: 4001,
	host: '0.0.0.0',
	endpoint: '/api/v1/jobs',
})
```

When a worker is started, it will connect to the orchestration node and begin polling for jobs to execute. When a job is completed, the worker will report back to the orchestration node and the job will be marked as completed.

To ensure that the queue remains aware of the worker's status, the worker will send a heartbeat to the orchestration node at the specified interval. If the worker does not send a heartbeat, the orchestration node will consider the worker disconnected and will stop sending it jobs.

The configuration object passed to `queue.worker.start()` must indicate to the orchestration node "how to reach" the worker. And conversely, the orchestration configuration in the `queue` definition must indicate to the worker "how to reach" the orchestration node.

---

## Observability and Monitoring

Observability is a first-class feature: every job emits detailed events (start, success, failure, pause, resume, cancel) along with metadata such as execution IDs and source code snapshots. A built-in web server can be spun up to display:

- **Workflow Overview**: List of active workflows, job states, retries, errors, and outcomes.
- **Job Traces**: Detailed execution logs and event logs.
- **Source Code Display**: Code snippets of each job's steps for debugging.

### Example

```ts
const queue = createQueue({
	jobs: { /* job definitions */ },
	orchestration: {
		port: 3000,
		host: '0.0.0.0',
		endpoint: '/jobs',
		interval: '1/s',
	},
	monitoring: {
		endpoint: '/monitoring',
	}
})

queue.orchestration.start()
queue.monitoring.start()
```

By default, the monitoring server will start with the same parameters as the orchestration server. This is because most of the time, it's fine to just run the monitoring server on the same node as the orchestration server.

But if you want to run the monitoring server on a different node, you can pass in a different configuration object to the `monitoring` property of the queue, and the monitoring node will forward all requests to the orchestration node.

---

## Example Workflow Patterns

Below are a few examples using the API:

### Conditional Branching Example

```ts
const checkThreshold = createJob({
    name: 'checkThreshold',
    input: z.object({ value: z.number() }),
}, async ({ value }) => {
    if (value > 10) {
        await highValue.run({ value });
    } else {
        await lowValue.run({ value });
    }
});

const highValue = createJob({
    name: 'highValue',
    input: z.object({ value: z.number() }),
}, async ({ value }) => {
    console.log(`High value detected: ${value}`);
});

const lowValue = createJob({
    name: 'lowValue',
    input: z.object({ value: z.number() }),
}, async ({ value }) => {
    console.log(`Low value detected: ${value}`);
});
```

### Fan-Out / Fan-In Example

```ts
const processPart1 = createJob({
    name: 'processPart1',
    input: z.object({ partData: z.string() }),
    output: z.object({ result: z.string() }),
}, async ({ partData }) => {
    return { result: partData + '_processed1' };
});

const processPart2 = createJob({
    name: 'processPart2',
    input: z.object({ partData: z.string() }),
    output: z.object({ result: z.string() }),
}, async ({ partData }) => {
    return { result: partData + '_processed2' };
});

const aggregateResults = createJob({
    name: 'aggregateResults',
    input: z.object({ results: z.array(z.string()) }),
}, async ({ results }) => {
    console.log('Aggregated results:', results.join(', '));
});

const fanOutInWorkflow = createJob({
    name: 'fanOutInWorkflow',
    input: z.object({ data: z.string() }),
}, async ({ data }) => {
    const [res1, res2] = await Promise.all([
        processPart1.run({ partData: data }),
        processPart2.run({ partData: data }),
    ]);
    await aggregateResults.run({ results: [res1.result, res2.result] });
});
```

### Error Handling / Compensation Example

```ts
const compensationJob = createJob({
    name: 'compensationJob',
    input: z.object({ taskId: z.string() }),
}, async ({ taskId }) => {
    console.log('Compensation action taken for', taskId);
});

const riskyJob = createJob({
    name: 'riskyJob',
    input: z.object({ taskId: z.string() }),
    onFailure: ({ taskId }) => compensationJob.start({ taskId }),
}, async ({ taskId }) => {
    // Simulate a failure 50% of the time
    if (Math.random() < 0.5) {
        throw new Error('Simulated failure');
    }
    console.log('Risky job succeeded for', taskId);
});

// Alternatively, attach error handling
riskyJob.on('failure', ({ taskId }) => {
    compensationJob.start({ taskId });
});
```





-----

## TODO docs

Here are the points I found that might be unclear or missing in the current documentation:
1. Job Execution Methods:
   - The distinction between using job.start() versus job.run() isn’t clearly defined. For example, when should a user call start() (which seems to return an execution object with lifecycle methods) versus run() (which appears to return a promise with the result)?
2. Global vs. Per‑Job Hooks:
   - While global hooks on the Queue are mentioned, it’s not entirely clear how they interact with or override per-job hooks.
   - There isn’t a detailed explanation of the event names, their payloads, or the order in which global hooks, per-job hooks, and explicit job handlers are executed.
3. Steps and Built‑in Methods:
   - The “Steps” section lists functions like run, thread, sleep, and pipe methods, but it doesn’t explain their intended use cases in depth. For instance, what’s the difference between using run() and thread() in practical scenarios?
   - It isn’t clear how errors and exceptions within these steps are propagated or handled.
4. Persistence Details:
   - While the available storage adapters (SQLite, PostgreSQL, in-memory, Redis) are mentioned, it isn’t explicit what happens by default if no adapter is provided.
   - There’s no discussion of the tradeoffs between these methods (for example, durability guarantees versus performance).
5. Workflow Composition & Complexity:
   - Although you mention that composite workflows are simply built by awaiting one job from another, there’s not much guidance on when more complex patterns (like fan-out/in or error compensation flows) might need extra attention or best practices.
   - It is left a bit ambiguous which patterns are best implemented using existing primitives versus when users might be forced to write extra boilerplate.
6. Deployment and Worker Integration:
   - The section on “Deployment and Scalability Considerations” provides an example of orchestration and workers but doesn’t clearly explain how the orchestration node tracks worker status (for example, the heartbeat mechanism).
   - The relationship between the orchestration node and the distributed workers (including how errors, retries, and job reassignment are managed) is only hinted at.
7. API Definitions and Options:
   - The API section under createJob, createQueue, and createPipe is incomplete in places (with ellipses hinting at missing details), so it isn’t clear what full options might be available.
   - For example, options for debounce, throttle, and the complete interface for RunOptions and ThreadOptions are not fully documented.
8. CronScheduler and Scheduling Options:
   - The QueueOptions mention a cronScheduler but provide little detail on how it is configured or how it integrates with job execution.
9. Error Propagation and Handling:
   - There isn’t a clear explanation of how errors inside a job’s handler are propagated to the lifecycle hooks or the promise returned by run(), especially in the context of retries and backoff strategies.
10. Type Definitions and Specific Interfaces:
   - Some types like Duration, ThreadContext, or ResourceLimits are mentioned without much explanation or examples, which might leave users unsure of how to supply these parameters or how they behave.

Addressing these points in the documentation would help clarify the intended usage and power of the API while making it easier for developers to understand the edge cases and best practices.
