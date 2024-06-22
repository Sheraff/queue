import EventEmitter from "events"
import type { Data, DeepPartial, Validator } from "./types"
import { Pipe } from "./pipe"
import { execution, registration } from "./context"

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: In]
	start: [data: In]
	success: [data: In, result: Out]
	error: [data: In, error: unknown]
	settled: [data: In, result: Out | null, error: unknown | null]
}

export type RunOptions = {
	id: string
	// timeout
	// retries
	// concurrency
	// ...
}

export type WaitForOptions<In extends Data> = {
	filter?: DeepPartial<In>
	timeout?: number
	future?: boolean
}

const job = Symbol('job')
export const fn = Symbol('fn')
export class Job<
	const Id extends string = string,
	In extends Data = Data,
	Out extends Data = Data,
> {
	readonly id: Id
	readonly in = null as unknown as In
	readonly out = null as unknown as Out
	readonly events = null as unknown as EventMap<In, Out>
	readonly #emitter = new EventEmitter<EventMap<In, Out>>()
	readonly #symbol = job
	readonly [fn]: (input: Data) => Promise<Data>

	readonly emitter = {
		on: this.#emitter.on.bind(this.#emitter),
		once: this.#emitter.once.bind(this.#emitter),
		off: this.#emitter.off.bind(this.#emitter),
		eventNames: this.#emitter.eventNames.bind(this.#emitter),
		listenerCount: this.#emitter.listenerCount.bind(this.#emitter),
		addListener: this.#emitter.addListener.bind(this.#emitter),
		removeListener: this.#emitter.removeListener.bind(this.#emitter),
	}

	constructor(
		opts: {
			id: Id
			input?: Validator<In>
			output?: Validator<Out>
			triggers?: NoInfer<Array<Pipe<string, In>>>
			cron?: string | string[]
		},
		job: (input: In) => Promise<Out>
	) {
		this.id = opts.id
		this[fn] = job as unknown as (input: Data) => Promise<Data>
	}

	dispatch(data: In): void {
		// should resolve which queue we're in and dispatch to that queue
		// if not resolved, throw error
		// - from other job => look if this job is also registered in the same queue
		// - from event listener on a job => look if this job is registered in the same queue
		// - from queue accessor `queue.jobs.[id].dispatch(data)` => use that queue
		const e = execution.getStore()
		if (e) throw new Error("Cannot call this method inside a job script. Prefer using `Job.dispatch()`, or calling it inside a `Job.run()`.")
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		// TODO: impl.
		return
	}

	static async run<Out extends Data>(id: string, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(opts: RunOptions, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(optsOrId: string | RunOptions, fn: () => Out | Promise<Out>): Promise<Out> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		const opts: RunOptions = typeof optsOrId === 'string' ? { id: optsOrId } : optsOrId
		return e.run(opts, fn)
	}

	static sleep(ms: number): Promise<void> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.sleep(ms)
	}

	static async waitFor<J extends Job, Event extends keyof EventMap<J['in'], J['out']>>(job: J, event?: Event, options?: WaitForOptions<J['in']>): Promise<EventMap<J['in'], J['out']>[Event]>
	static async waitFor<P extends Pipe>(pipe: P, options?: WaitForOptions<P['in']>): Promise<P['in']>
	static async waitFor(instance: Job | Pipe, eventOrOptions?: string | Data, jobOptions?: Data): Promise<Data> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		const options = instance instanceof Pipe ? eventOrOptions : jobOptions
		const event = instance instanceof Pipe ? 'success' : eventOrOptions ?? 'success'
		return e.waitFor(
			instance,
			event as unknown as keyof EventMap<any, any>,
			options as unknown as WaitForOptions<Data>
		)
	}

	static async invoke<J extends Job>(job: J, data: J['in']): Promise<J['out']> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.invoke(job, data)
	}

	static dispatch<I extends Job | Pipe>(instance: I, data: I['in']): void {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.dispatch(instance, data)
	}
}