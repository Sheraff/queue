import { registration } from "./context"
import type { Data, DeepPartial, Validator } from "./types"



const pipe = Symbol('pipe')
export class Pipe<
	const Id extends string = string,
	In extends Data = Data,
> {
	readonly id: Id
	readonly in = null as unknown as In
	readonly #symbol = pipe

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

	dispatch(data: In): void {
		// should resolve which queue we're in and dispatch to that queue
		// if not resolved, throw error
		// - from job => look if this pipe is also registered in the same queue
		// - from event listener on a job => look if this pipe is registered in the same queue
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return
	}
	waitFor(filter?: DeepPartial<In>): Promise<In> {
		// if not in job queue context, throw error
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return {} as Promise<In>
	}
}