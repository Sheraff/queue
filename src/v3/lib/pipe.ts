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
		return
	}
	waitFor(filter?: DeepPartial<In>): Promise<In> {
		return {} as Promise<In>
	}
}