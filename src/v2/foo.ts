import Database from 'better-sqlite3'
import { z } from "zod"

const db: Database.Database = new Database('woop.db', {})
db.pragma('journal_mode = WAL')
db.exec(/* sql */ `
	CREATE TABLE IF NOT EXISTS tasks (
		id INTEGER PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS memo (
		id INTEGER PRIMARY KEY
	);

`)

type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | GenericSerializable[] | { [key: string]: GenericSerializable }

type Data = GenericSerializable


type ProgramTriggers<Events extends string = never> = {
	event?: Events | Events[]
	cron?: string | string[]
}

type ConcurrencyOptions = {
	/** Globally unique identifier for the concurrency check, defaults to `id` of the program */
	id?: string
	/** How many calls to run concurrently. Defaults to `1`. */
	limit?: number
	/** How long to wait before running the next batch of calls. */
	delay?: number
}

type ProgramTimings = {
	/** How long to wait before running the program. If other calls are made before this time, the timer is reset. */
	debounce?: number
	/** How long before the program is considered to have timed out, and should be cancelled. */
	timeout?: number
	/** How long to wait between each call to the program. If other calls are made before this time, they are dropped. */
	throttle?: number
	/** `input` will be an array of inputs batched over time. */
	batch?: {
		/** How many calls to batch together. */
		size: number
		/** How long to wait before sending a batch, even if it's not full. */
		timeout?: number
	}
	concurrency?: number | ConcurrencyOptions | ConcurrencyOptions[]
}

type ProgramRetry = {
	/** How many times to retry the program before giving up. */
	attempts?: number
	/** How long to wait between each retry. */
	delay?: number | ((attempt: number) => number)
}

type Validator<Out = Data> = {
	parse: (input: any) => Out
}

type ProgramOptions<In extends Data = Data, Out extends Data = Data, Events extends string = never, Id extends string = never> = {
	/** globally unique identifier for this workflow */
	id: Id
	triggers?: ProgramTriggers<Events>
	timings?: ProgramTimings
	input?: Validator<In>
	output?: Validator<Out>
	retry?: ProgramRetry
	priority?: number | NoInfer<((input: In) => number)>
	onSuccess?: NoInfer<(output: Out) => void>
	onError?: (error: Error) => void
	onSettled?: NoInfer<(error: Error | null, output: Out | null) => void>
	onTimeout?: () => void
	onCancel?: () => void
	onRetry?: NoInfer<(error: Error, attempt: number) => void>
}

type ProgramFn<In extends Data = Data, Out extends Data = Data> = (
	input: In
) => Promise<Out>

interface Program<In extends Data = Data, Out extends Data = Data, Events extends string = never, Id extends string = never> {
	readonly invoke: (input: In) => Promise<Out>
	readonly id: Id
	readonly opts: {}
	readonly __in: In
	readonly __out: Out
	readonly __events: Events
}

function createProgram<In extends Data = Data, Out extends Data = Data, Events extends string = never, const Id extends string = never>(
	config: ProgramOptions<In, Out, Events, Id>,
	fn: ProgramFn<In, Out>
): NoInfer<Program<In, Out, Events, Id>> {
	const opts = Object.assign({
		triggers: {},
		timings: {},
		retry: {
			attempts: 1,
			delay: 1000,
		},
	}, typeof config === 'string' ? { id: config } : config)
	return {
		id: opts.id,
		opts,
		fn,
	} as unknown as Program<In, Out, Events, Id>
}



const step = {} as Utils

const pokemon = createProgram({
	id: 'pokemon',
	output: z.object({ name: z.string() }),
	input: z.object({ id: z.number() }),
	triggers: { event: 'poke' }
}, async (input) => {

	const data = await step.run({ name: 'fetch', concurrency: { id: 'pokeapi' } }, async () => {
		const response = await fetch(`https://pokeapi.co/api/v2/pokemon/${input.id}`)
		return response.json() as Promise<{ name: string, order: number }>
	})

	step.dispatchProgram('foo', { fa: '1' })
	step.dispatchProgram(pokemon, { id: 23 })
	step.dispatchProgram(foo, { fa: '1' })
	const eventData = await step.waitForEvent('foo-trigger')

	step.dispatchEvent('poke', { fa: '1', id: 12 })

	const dodo = await step.invokeProgram('foo', { fa: '1' })
	const dudu = await step.invokeProgram(pokemon, { id: 23 })

	return { name: data.name }
})

const foo = createProgram({
	id: 'foo',
	input: z.object({ fa: z.string() }),
	output: z.object({ fi: z.string() }),
	triggers: { event: ['foo-trigger', 'poke'] }
}, async (input) => {
	return { fi: input.fa }
})

pokemon.invoke({ id: 23 })

function registerPrograms<
	const Dict extends {
		[Key in string]: Program<any, any, any, Key>['id'] extends Key ? Program<any, any, any, Key> : never
	}
>(
	dictionary: Dict
): Dict {
	return dictionary as Dict
}

// function registerPrograms<PS extends Program<any, any, any, any>[]>(...programs: PS): MapId<PS> {
// 	return programs.reduce((acc, cur) => {
// 		acc[cur.id] = cur
// 		return acc
// 	}, {} as any)
// }

const registry = registerPrograms({
	pokemon,
	foo,
})

declare global {
	interface Registry2 {
		registry: typeof registry
	}
}

type UnionToIntersection<U> =
	(U extends any ? (x: U) => void : never) extends ((x: infer I) => void) ? I : never

type RegistryEvents = {
	[event in Registry2['registry'][keyof Registry2['registry']]['__events']]: UnionToIntersection<{
		[key in keyof Registry2['registry']]: event extends Registry2['registry'][key]['__events'] ? Registry2['registry'][key]['__in'] : never
	}[keyof Registry2['registry']]>
}

type RegistryPrograms = {
	[id in keyof Registry2['registry']]: {
		p: Registry2['registry'][id]
		__in: Registry2['registry'][id]['__in']
		__out: Registry2['registry'][id]['__out']
	}
}

interface Utils {
	run<T extends Data>(name: string, fn: () => Promise<T>): Promise<T>
	run<T extends Data>(opts: {
		name: string
		retry?: ProgramRetry
		concurrency?: ConcurrencyOptions
	}, fn: () => Promise<T>): Promise<T>
	sleep(ms: number): Promise<void>
	/** dispatch an event */
	dispatchEvent<Event extends keyof RegistryEvents>(name: Event, data: RegistryEvents[Event]): void
	waitForEvent<Event extends keyof RegistryEvents>(name: Event): Promise<RegistryEvents[Event]>
	waitForEvent<Event extends keyof RegistryEvents>(opts: {
		name: Event
		timeout?: number
	}): Promise<RegistryEvents[Event]>
	/**  */
	dispatchProgram<P extends keyof RegistryPrograms>(id: P, input: RegistryPrograms[P]['__in']): void
	dispatchProgram<P extends keyof RegistryPrograms>(program: RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): void
	invokeProgram<P extends keyof RegistryPrograms>(id: P, input: RegistryPrograms[P]['__in']): Promise<RegistryPrograms[P]['__out']>
	invokeProgram<P extends keyof RegistryPrograms>(program: RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): Promise<RegistryPrograms[P]['__out']>
}