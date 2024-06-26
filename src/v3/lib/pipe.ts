import { registration } from "./context"
import type { InputData, Validator } from "./types"
import { getRegistrationContext } from "./utils"

export type PipeInto<In extends InputData, Out extends InputData> = [
	pipe: Pipe<string, In>,
	transform: (input: In) => Out
]

export class Pipe<
	const Id extends string = string,
	In extends InputData = InputData,
> {
	/** @public */
	readonly id: Id
	/** @package */
	readonly in = null as unknown as In
	/** @public */
	readonly type = 'pipe'

	constructor(
		opts: {
			id: Id,
		} & (
				| { in: In, input?: never }
				| { in?: never, input: Validator<In> }
			)
	) {
		this.id = opts.id
	}

	/**
	 * @public
	 * 
	 * Getter for the parent `queue` in the current context.
	 * 
	 * ```ts
	 * myQueue.pipes.myPipe.queue === myQueue
	 * ```
	 * 
	 * @throws {ReferenceError} Will throw an error if called outside of a queue context.
	 */
	get queue() {
		return getRegistrationContext(this).queue
	}

	/**
	 * @public
	 *
	 * Dispatches the input data into the pipe.
	 * 
	 * Any job that is connected to this pipe will be triggered,
	 * - either through a `Job.waitFor` step,
	 * - or through a `triggers: [pipe]` config.
	 * 
	 * Dispatch should only be called from within the context of a queue:
	 * 
	 * If you're calling this method from within a job, it will already be in the context of a queue:
	 * ```ts
	 * Job.run('my-step', () => {
	 *   myPipe.dispatch({ foo: 'bar' })
	 * })
	 * ```
	 * 
	 * If you're calling this method from anywhere else, just access the pipe through the queue:
	 * ```ts
	 * queue.pipes.myPipe.dispatch({ foo: 'bar' })
	 * ```
	 */
	dispatch(input: In): void {
		const registrationContext = getRegistrationContext(this)
		const string = JSON.stringify(input)
		registrationContext.recordEvent(`pipe/${this.id}`, string, string)
		registrationContext.triggerJobsFromPipe(this, input)
		return
	}

	/**
	 * @public
	 * 
	 * When a job is triggered by this pipe, it is possible that the pipe's input data
	 * does not match the job's input data. This method allows you to transform the data accordingly.
	 * 
	 * ```ts
	 * const myJob = new Job({
	 *   id: 'myJob',
	 *   triggers: [myPipe.into((input) => ({ bar: String(input.foo) }))]
	 * }, ...)
	 * ```
	 */
	into<T extends InputData>(cb: (input: In) => T): PipeInto<In, T> {
		return [this, cb]
	}
}
