import { AsyncLocalStorage } from "async_hooks"
import type { Queue } from "./queue"
import type { Pipe } from "./pipe"
import type { Job, RunOptions, WaitForOptions } from "./job"
import type { Data } from "./types"
import type { Step, Task } from "./storage"

export interface RegistrationContext {
	queue: Queue
	checkRegistration(instance: Job<any, any, any> | Pipe<any, any>): void | never
	addTask<T>(job: Job, data: Data, cb: (key: string, inserted: boolean) => T): T | Promise<T>
	resolveTask<T>(task: Task, status: 'completed' | 'cancelled', data: Data, cb: () => T): T | Promise<T>
	resolveTask<T>(task: Task, status: 'failed', data: unknown, cb: () => T): T | Promise<T>
	requeueTask<T>(task: Task, cb: () => T): T | Promise<T>
	recordStep<T>(job: Job, task: Task, step: Pick<Step, 'step' | 'status' | 'data' | 'sleep_for'>, cb: () => T): T | Promise<T>
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
	sleep(ms: number): Promise<void>
	waitFor(instance: Job | Pipe, event: string, options?: WaitForOptions<Data>): Promise<Data>
	invoke(job: Job, data: Data): Promise<Data>
	dispatch(instance: Job | Pipe, data: Data): void
}

/**
 * Context provided to bridge over user code at the root level of a job script
 * - defined in `Job({}, () => { <here> })`
 * - undefined inside `Job.run({}, () => { <here> })`
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
export const execution = new AsyncLocalStorage<ExecutionContext | null>()