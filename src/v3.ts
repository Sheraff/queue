import EventEmitter from "events"
import { z } from "zod"

type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | void | GenericSerializable[] | { [key: string]: GenericSerializable }

type Data = GenericSerializable

type Validator<Out = Data> = {
	parse: (input: any) => Out
}

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: In]
	start: [data: In]
	success: [data: In, result: Out]
	error: [data: In, error: unknown]
	settled: [data: In, result: Out | null, error: unknown | null]
}

type BatchOptions = {
	max?: number
	timeout?: number
}

type BatchArray<Batch extends object | undefined, T> = Batch extends object ? T[] : T

const program = Symbol('program')
class Program<
	const Id extends string = string,
	In extends Data = Data,
	Out extends Data = Data,
	Batch extends BatchOptions | undefined = undefined,
> extends EventEmitter<
	EventMap<In, Out>
> {
	readonly id: Id
	readonly in = null as unknown as In
	readonly out = null as unknown as Out
	readonly events = null as unknown as EventMap<In, Out>
	readonly #symbol = program
	constructor(
		opts: {
			id: Id
			input?: Validator<BatchArray<Batch, In>>
			output?: Validator<BatchArray<Batch, Out>>
			batch?: Batch & BatchOptions
			triggers?: NoInfer<Array<Pipe<string, In>>>
			cron?: string | string[]
		},
		fn: (input: BatchArray<Batch, In>) => Promise<BatchArray<Batch, Out>>
	) {
		super()
		this.id = opts.id
	}

	invoke(data: In): Promise<Out> {
		return {} as Promise<Out>
	}
	dispatch(data: In): void {
		return
	}
	waitFor<Event extends keyof EventMap<In, Out>>(event: Event, input: In): Promise<EventMap<In, Out>[Event]> {
		return {} as any
	}
}


const pipe = Symbol('pipe')
class Pipe<
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
	waitFor(): Promise<In> {
		return {} as Promise<In>
	}
}

const dada = new Pipe({
	id: 'dada',
	input: z.object({ c: z.string() }),
})

const titi = new Pipe({
	id: 'titi',
	in: {} as { a: string },
})

const aaa = new Program({
	id: 'aaa',
	input: z.object({ a: z.string() }),
	output: z.object({ b: z.string() }),
	triggers: [titi],
}, async ({ a }) => {
	const foo = await bbb.invoke({ b: '1' })
	return { b: foo.c }
})

const bbb = new Program({
	id: 'bbb',
}, async ({ b }: { b: string }): Promise<{ c: string }> => {
	const foo = await aaa.invoke({ a: '1' })
	const [data, result, error] = await aaa.waitFor('settled', { a: '1' })
	const evData = await dada.waitFor()
	ccc.dispatch({ c: '1' })
	return { c: foo.b }
})
aaa.on('success', (data, result) => bbb.dispatch(result))


// TODO: there are issues with the batch options: without `batch` defined, it doesn't seem to accept arrays as input
const ccc = new Program({
	id: 'ccc',
	input: z.array(z.object({ c: z.string() })),
	output: z.array(z.object({ d: z.string() })),
	batch: {
		max: 10,
		timeout: 1000,
	},
}, async (inp) => {
	return [{ d: inp[0]!.c }]
})

type SafeKeys<K extends string> = { [k in K]: { id: k } }

class Queue<
	const Programs extends { [key in string]: Program<key> },
	const Pipes extends { [key in string]: Pipe<key> },
> {
	public readonly programs: Programs
	public readonly pipes: Pipes
	constructor(opts: {
		programs: Programs & SafeKeys<keyof Programs & string>,
		pipes: Pipes & SafeKeys<keyof Pipes & string>
	}) {
		this.programs = opts.programs
		this.pipes = opts.pipes
	}
}

const queue = new Queue({
	programs: {
		aab: aaa,
		bbb,
		ccc,
	},
	pipes: {
		dada: dodo,
		titi,
	}
})

queue.programs.bbb.invoke({ b: 'oih' })



aaa.on('start', (data) => bbb.dispatch({ b: data.a }))


const em = new EventEmitter()