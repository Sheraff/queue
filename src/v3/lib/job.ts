import EventEmitter from "events"
import type { Data, DeepPartial, Validator } from "./types"
import type { Pipe } from "./pipe"
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
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return
	}
	invoke(data: In): Promise<Out> {
		// TODO: should this be moved to a Job static method? It should never be called outside of a job script, same as `Job.run` and `Job.sleep`
		// if not in job queue context, throw error
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return {} as Promise<Out>
	}
	waitFor<Event extends keyof EventMap<In, Out>>(event: Event, filter: DeepPartial<In>): Promise<EventMap<In, Out>[Event]> {
		// TODO: should this be moved to a Job static method? It should never be called outside of a job script, same as `Job.run` and `Job.sleep`
		// if not in job queue context, throw error
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return {} as any
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
}