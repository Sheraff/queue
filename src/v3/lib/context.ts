import { AsyncLocalStorage } from "async_hooks"
import type { Queue } from "./queue"
import type { Pipe } from "./pipe"
import type { CancelReason, Job, RunOptions, WaitForOptions } from "./job"
import type { Data, InputData } from "./types"
import type { Step, Task } from "./storage"

export interface RegistrationContext {
	queue: Queue
	checkRegistration(
		instance: Job<any, any, any> | Pipe<any, any>
	): void | never
	addTask<T>(
		job: Job,
		data: Data,
		key: string,
		parent: number | undefined,
		priority: number,
		debounce: undefined | { s: number, id: string },
		throttle: undefined | { s: number, id: string },
		rateLimit: undefined | { s: number, id: string },
		timeout: number | null,
		cb: (rateLimit: number | null, inserted: boolean, cancelled?: Task) => T
	): T | Promise<T>
	resolveTask<T>(
		task: { queue: string, job: string, key: string },
		status: 'completed' | 'cancelled',
		data: Data,
		cb: () => T
	): T | Promise<T>
	resolveTask<T>(
		task: { queue: string, job: string, key: string },
		status: 'failed',
		data: unknown,
		cb: () => T
	): T | Promise<T>
	requeueTask<T>(
		task: Task,
		cb: () => T
	): T | Promise<T>
	recordStep<T>(
		task: Task,
		step: Pick<Step, 'step' | 'status' | 'next_status' | 'data' | 'wait_for' | 'wait_filter' | 'wait_retroactive' | 'runs'> & { sleep_for?: number | null },
		cb: () => T
	): T | Promise<T>
	recordEvent(
		key: string,
		input: string,
		data: string
	): void
	triggerJobsFromPipe(
		pipe: Pipe<string, any>,
		input: InputData
	): void
}

/**
 * Context provided to connect job graph to the queue from which it was dispatched
 * - defined in `job.dispatch()`, `pipe.dispatch()`, `Job({}, () => { <here> })`
 * - never undefined, remains accessible in event listeners to enable sub-actions
 *
 * ```ts
 * queue.jobs.myJob.emitter.on('success', () => otherJob.dispatch())
 * //                                           ^? this job will be started `queue`
 * ```
 * ```ts
 * Job.run({}, () => otherJob.dispatch())
 * //                ^? this job will be started in the current Queue
 * ```
 *
 * Defined by `Queue` at the start of a branch
 */
export const registration = new AsyncLocalStorage<RegistrationContext>()


export interface ExecutionContext {
	run<Out extends Data>(options: RunOptions, fn: () => Out | Promise<Out>): Promise<Out>
	sleep(ms: number): Promise<void> | void
	waitFor(instance: Job | Pipe, event: string, options: WaitForOptions<InputData>): Promise<Data> | void
	invoke(job: Job, data: InputData): Promise<Data>
	dispatch(instance: Job | Pipe, data: InputData): Promise<void>
	cancel(instance: Job, input: InputData, reason: CancelReason): Promise<void>
	promises: Promise<unknown>[]
	cancelled: boolean
}

/**
 * Context provided to bridge over user code at the root level of a job script
 * - defined in `Job({}, () => { <here> })`
 * - number (storage task id) inside `Job.run({}, () => { <here> })`
 * 
 * ```ts
 * new Job({}, async () => {
 *   await Job.run({}, () => { ... })
 *   //        ^? this will be able to access the current ExecutionContext
 * })
 * ```
 * ```ts
 * new Job({}, async () => {
 *   await Job.run({}, () => {
 *     await Job.run({}, () => { ... })
 *     //    ^? this will crash because of the absence of ExecutionContext
 *   })
 * })
 * ```
 *
 * Defined by `Queue` at the start of execution, used by `Job.[run|sleep|...]`
 */
export const execution = new AsyncLocalStorage<ExecutionContext | number>()