import { execution, registration } from "./context"
import type { Data, Validator } from "./types"

export class Pipe<
	const Id extends string = string,
	In extends Data = Data,
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

	/** @public */
	dispatch(data: In): void {
		// should resolve which queue we're in and dispatch to that queue
		// if not resolved, throw error
		// - from job => look if this pipe is also registered in the same queue
		// - from event listener on a job => look if this pipe is registered in the same queue
		const e = execution.getStore()
		if (e) throw new Error("Cannot call this method inside a job script. Prefer using `Job.dispatch()`, or calling it inside a `Job.run()`.")
		const store = registration.getStore()
		if (!store) throw new Error("Cannot call this method outside of the context of a queue.")
		store.checkRegistration(this)
		return
	}
}