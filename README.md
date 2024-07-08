# Asynchronous Task Queue System with TypeScript

*Background jobs, asynchronous queue, task scheduling, durable execution, type-safe, event-driven orchestration, self-hosted, sqlite, human-in-the-loop*

This project provides a flexible and efficient task queue system for managing asynchronous jobs and workflows. It is built with TypeScript and designed to handle complex job orchestration, event-driven workflows, and durable execution of tasks. The system supports parallel execution, retries, timeouts, and customizable backoff strategies, making it suitable for a wide range of applications.

It is heavily inspired by [Inngest](https://www.inngest.com/), and far inferior to it in every way.

## Example

Here's an example of a complex job that processes a song file, extracting metadata, cover art, and color palette, and then stores the information in a database. The job uses other jobs to fetch data from various APIs, extract color information from the cover art, and notify the user upon completion.

```ts
// Define the main job that processes a song file, will be explicitly triggered by application code
const processSong = new Job({
  id: 'processSong',
  input: z.object({ path: z.string() }),
}, async ({ path }) => {
  // Run a binary to extract audio fingerprint from the song file
  const fingerprint = await Job.run('fpcalc', async () => {
    return new Promise(resolve => {
      const child = spawn('fpcalc', [path])
      child.stdout.on('data', data => {
        resolve(data.toString().trim())
      })
    })
  })
  
  // Invoke the musicBrainz job to get the song ID
  const id = await Job.invoke(musicBrainz, { fingerprint })

  // Fetch data from various APIs in parallel, each with its own rate limit
  // Jobs are cached, so they won't be re-run if the same ID is processed again
  const all = await Promise.all([
    Job.invoke(spotifyData, { id }),
    Job.invoke(lastFmData, { id }),
    Job.invoke(audioDbData, { id }),
  ])
  const metadata = parseMetadata(all)
  
  // Download the cover art
  const coverArtPath = await Job.run('downloadCoverArt', async () => {
    const { path } = await download(metadata.coverArtUrl)
    return path
  })

  // Extract the color palette
  const palette = await Job.thread('extractPalette', ({ path }) => {
    // This is a CPU-intensive operation, so we run it in a separate thread
  }, { path: coverArtPath })

  // Store the song information in the database
  await Job.run('storeSong', async () => {
    await db.run('INSERT INTO songs VALUES (?, ?, ?, ?, ?)', [
      id,
      metadata.title,
      metadata.artist,
      coverArtPath,
      palette
    ])
  })

  // Send a push notification to the user
  await Job.run('notifyUser', async () => {
    await sendNotification('Song processed', `Song ${metadata.title} by ${metadata.artist} has been processed`)
  })
})

const musicBrainz = new Job({
  id: 'musicBrainz',
  input: z.object({ fingerprint: z.string() }),
  output: z.string(),
  throttle: "1 per second", // rate limit of API
}, async ({ fingerprint }) => {
  const data = await Job.run('musicBrainzApi', async () => {
    const response = await fetch(`https://musicbrainz.org/api/${fingerprint}`)
    return response.json()
  })
  const id = parseMusicbrainzData(data)
  return id
})

// other jobs not shown for brevity
const spotifyData = new Job(...) // fetch job throttled based on API rate limits
const lastFmData = new Job(...) // fetch job throttled based on API rate limits
const audioDbData = new Job(...) // fetch job throttled based on API rate limits
```

## Features
- Flexible Job Handling: Supports retries with customizable backoff strategies.
- Event-Driven Orchestration: Listen to and trigger events based on job execution results.
- Type Safety: Built with TypeScript, ensuring type safety across the board.
- Customizable Execution Context: Tailor the execution context to fit the needs of each job.
- Efficient Storage Management: Utilizes better-sqlite3* for fast and reliable storage of jobs and their states.

*Note: copilot wrote this, but really the storage part is abstracted and can be swapped out for other implementations, including asynchronous like redis, or even over-the-network like a REST API or a Postgres database.

## Core Concepts

### Queue
The [`Queue`](src/lib/queue.ts) class is the heart of the task queue system. It manages the lifecycle of jobs and pipes, ensuring that tasks are executed efficiently and in order. A `Queue` instance can handle multiple jobs and pipes simultaneously, supporting parallel execution and sophisticated scheduling strategies. It also interacts with the storage system to persist job states and outcomes.

Key Features:
- Parallel execution of jobs with configurable concurrency limits.
- Event-driven architecture, allowing jobs to trigger other jobs or actions upon completion.
- Efficient storage management for job states and history.
- Flexible job and pipe registration system.

### Job
The [`Job`](src/lib/job.ts) class represents a workflow / task within the queue system. Each job has a unique identifier and a set of triggers that can initiate its execution (including explicitly calling it from application code). Jobs can be configured with various orchestration settings (timeout, debounce, rate-limit, ...). Within a job, we use static `Job` class methods to schedule units of work, wait for other jobs or events, and manage the job's lifecycle.

Key Features:
- Unique identification and configurable execution context.
- Support for orchestration strategies.
- Ability to wait for other jobs or external events before proceeding.
- Integration with the `Pipe` system for event-driven orchestration.

### Pipe
The [`Pipe`](src/lib/pipe.ts) class facilitates communication and data flow between jobs. It acts as a conduit, allowing jobs to pass data to each other in a decoupled type-safe manner. Pipes can be used to trigger jobs based on the completion of other jobs, enabling complex workflows and dependencies to be managed with ease.

Key Features:
- Decouples job execution and data dependencies.
- Enables event-driven job orchestration.
- Supports type-safe data passing between jobs.

## Example Usage

This example demonstrates how to create a simple workflow using [`Queue`]("src/lib/queue.ts"), [`Job`]("src/lib/job.ts"), and [`Pipe`]("src/lib/pipe.ts") to process tasks asynchronously.

### Defining Jobs and Pipes

First, define a [`Pipe`]("src/lib/pipe.ts") to pass data between jobs, and then define two jobs that interact through this pipe.

```ts
import { Job, Pipe, Queue } from './src/lib'

// Define a pipe to pass data between jobs
const dataPipe = new Pipe({
  id: 'dataPipe',
  in: {} as { message: string },
})

// Define a job that sends data to the pipe
const producerJob = new Job({
  id: 'producerJob',
}, async () => {
  const message = 'Hello from producer!'
  Job.dispatch(dataPipe, { message })
  return 'Producer job completed'
})

// Define a job that receives data from the pipe
const consumerJob = new Job({
  id: 'consumerJob',
  triggers: [dataPipe.into(({ message }) => ({ str: message }))],
  input: z.object({ str: z.string() }),
}, async (input) => {
  console.log(`Consumer received message: ${input.str}`)
  return 'Consumer job completed'
})
```

### Setting Up the Queue

Next, set up a `Queue` to manage these jobs and start processing tasks.

```ts
// Create a queue with the defined jobs and pipe
const queue = new Queue({
  jobs: { producerJob, consumerJob },
  pipes: { dataPipe },
  storage: new SqliteStorage()
})

// Wait for the queue to be ready
await queue.ready

// Dispatch the producer job to initiate the workflow
queue.jobs.producerJob.dispatch()
```

This setup demonstrates a basic workflow where `producerJob` sends a message to `consumerJob` through `dataPipe`. The [`Queue`]("src/lib/queue.ts") manages the execution of these jobs, ensuring that `consumerJob` is triggered after `producerJob` completes its task and dispatches the message.

> [!NOTE]
> The `SqliteStorage` class is used to store job states and history. It uses better-sqlite3, but can be swapped out for other storage implementations if needed.

### Job Steps

A Job represents a linear piece of work. It is just meant as an orchestration container for smaller units of work represented by the various `Job` static methods. Here's an example of a complex job that uses these methods to script a workflow:

```ts
const complexJob = new Job({
  id: 'complexJob',
}, async () => {
  // Job.run: Executes a function immediately, with retries and backoff.
  const runResult = await Job.run('subTask', () => {
    // ...
    return 'Sub-task completed'
  })

  // Job.thread: Similar to Job.run, but runs the function in a separate thread.
  const threadResult = await Job.thread('heavyTask', ({ arg }) => {
    // ...
    return 'Heavy task completed'
  }, { arg })

  // Job.sleep: Pauses the job execution for a specified duration.
  await Job.sleep("1s") // Sleep for 1 second

  // Job.waitFor: Waits for another job to reach a specific state before continuing.
  // Assuming there's another job 'preliminaryJob' that we wait to finish.
  const preliminaryResult = await Job.waitFor(preliminaryJob, 'success')

  // Job.waitFor: Can also be used to wait for a Pipe to receive data.
  // Assuming there's a Pipe 'dataPipe' that we wait to receive data.
  const data = await Job.waitFor(dataPipe)

  // Job.invoke: Invokes another job with the provided input and waits for its completion.
  const invokeResult = await Job.invoke(anotherJob, { somekey: 'data' })

  // Job.dispatch: Dispatches another job for execution without waiting for its completion.
  Job.dispatch(asyncJob, { startData: 'start' })

  // Job.cancel: Attempts to cancel another job.
  // Assuming 'longRunningJob' is a job that might still be running.
  await Job.cancel(longRunningJob, { input: 'foo' })

  return { runResult, preliminaryResult, invokeResult }
})
```

### Job Options
When creating a Job, you can customize its behavior by passing various options. Here's a breakdown of the options you can use:

- `input`: A Zod-compatible validator that validates the input data for the job. This ensures that the job receives the correct type of data before execution.
- `output`: A Zod-compatible validator that validates the output data of the job. This is useful for ensuring the job produces the expected output.
- `triggers`: An array of `Pipe` or `Pipe.into` that specifies what triggers the job. These pipes can be used to start the job based on events or conditions in other parts of your application.
- `priority`: A number or a function that returns a number, determining the priority of the job execution. Higher priority jobs are executed before lower priority ones. The function variant allows dynamic calculation of priority based on the job's input.
- `cron`: A cron expression or an array of cron expressions that schedule the job to run at specific times. This option is only available if the job's input type includes a date string (or doesn't expect an input at all).
- `debounce`: An OrchestrationTimer* that specifies a debounce strategy for the job. This can be used to limit how often the job can be triggered within a certain timeframe.
- `throttle`: An OrchestrationTimer* that specifies a throttle strategy for the job. Throttling ensures that the job does not run too frequently over a specified period.
- `rateLimit`: An OrchestrationTimer* that specifies a rate limit for the job. This can be used to restrict the number of times the job can be executed within a certain timeframe.
- `timeout`: A number, a string like "3h", or a function that returns those, specifying the maximum time the job is allowed to run. If the job exceeds this time, it will be terminated.


> [!NOTE]
> `OrchestrationTimer` can be a number, or a string like "2 per second", or a function that returns those.

Example:
```ts
const job = new Job({
  id: 'job',
  input: z.object({ data: z.string() }),
  output: z.object({ result: z.string() }),
  triggers: [pipe.into(({ foo }) => ({ data: foo }))],
  priority: 10,
  cron: '0 * 1 * *',
  throttle: '1 per minute',
  timeout: '1h',
}, async ({ data }) => {
  // Job logic here
  return { result: 'Job completed' }
})
```

These options provide a flexible way to control the execution and behavior of jobs within your application, allowing for complex workflows and job management strategies.

### Job steps with `Job.run`

When using the `Job.run` method, you can customize its behavior by passing an object of options. These options allow you to control aspects such as retry logic, backoff strategy, and execution timeout. Here's a detailed explanation of each option:

- `retry`: This option specifies the number of attempts to run the job, including the first one. By default, it is set to 3 attempts. You can provide a number directly or a function that decides whether to retry based on the current attempt number and the error that caused the previous attempt to fail. If the number is below 1, it is treated as 1.
- `backoff`: This option controls the delay before the next attempt is made after a failure. The delay can be specified in several ways:
    - As a direct number in milliseconds.
    - As a Duration string (e.g. "1h", "20 sec", ...).
    - As a function that returns a number or Duration, which is called with the number of times the step has been run already.
    - As an array of numbers or Duration objects, acting as a table of delays. The attempt number is used as the lookup index, with the last value repeating indefinitely if more attempts are made.
    - By default, the backoff strategy uses a list of increasing delays: "100ms", "30s", "2m", "10m", "30m", "1h", "2h", "12h", "1d".
- `timeout`: This option sets the maximum duration allowed for the job to run. If the job exceeds this time, it will be terminated. The timeout can be specified as a number in milliseconds or as a Duration object.

Example:
```ts
const runResult = await Job.run({
  id: 'subTask',
  retry: 5,
  backoff: '1s',
  timeout: '5s',
}, async () => {
  // Step logic here
  return 'Sub-task completed'
})
```

These options provide a flexible way to manage the execution of jobs, especially in scenarios where tasks might fail and require retries with a sensible backoff strategy.

## Observability

<img width="1800" alt="Screenshot 2024-07-08 at 16 53 20" src="https://github.com/Sheraff/queue/assets/1325721/73e88a1d-d24a-4915-b9df-14d7075fe5dd">

<img width="1781" alt="Screenshot 2024-07-08 at 16 54 16" src="https://github.com/Sheraff/queue/assets/1325721/3d9c5c25-6747-4d47-b917-d65303483638">








