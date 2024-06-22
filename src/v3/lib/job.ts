import EventEmitter from "events"
import type { Data, DeepPartial, Validator } from "./types"
import type { Pipe } from "./pipe"

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: In]
	start: [data: In]
	success: [data: In, result: Out]
	error: [data: In, error: unknown]
	settled: [data: In, result: Out | null, error: unknown | null]
}

const job = Symbol('job')
export class Job<
	const Id extends string = string,
	In extends Data = Data,
	Out extends Data = Data,
> {
	readonly id: Id
	readonly in = null as unknown as In
	readonly out = null as unknown as Out
	readonly events = null as unknown as EventMap<In, Out>
	readonly emitter = new EventEmitter<EventMap<In, Out>>()
	readonly #symbol = job
	constructor(
		opts: {
			id: Id
			input?: Validator<In>
			output?: Validator<Out>
			triggers?: NoInfer<Array<Pipe<string, In>>>
			cron?: string | string[]
		},
		fn: (input: In) => Promise<Out>
	) {
		this.id = opts.id
	}

	invoke(data: In): Promise<Out> {
		return {} as Promise<Out>
	}
	dispatch(data: In): void {
		return
	}
	waitFor<Event extends keyof EventMap<In, Out>>(event: Event, filter: DeepPartial<In>): Promise<EventMap<In, Out>[Event]> {
		return {} as any
	}

	static run<Out extends Data>(id: string, fn: () => Out | Promise<Out>): Promise<Out>
	static run<Out extends Data>(opts: { id: string }, fn: () => Out | Promise<Out>): Promise<Out>
	static run<Out extends Data>(id: string | { id: string }, fn: () => Out | Promise<Out>): Promise<Out> {
		return {} as Promise<Out>
	}

	static sleep(ms: number): Promise<void> {
		return {} as Promise<void>
	}
}