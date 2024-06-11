import { AsyncLocalStorage } from "node:async_hooks"
import Database from 'better-sqlite3'
import { z } from "zod"
import EventEmitter from "node:events"

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

type GenericSerializable = Scalar | undefined | void | GenericSerializable[] | { [key: string]: GenericSerializable }

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
	onTrigger?: NoInfer<(input: In) => void>
	onStart?: NoInfer<(input: In) => void>
	onSuccess?: NoInfer<(input: In, output: Out) => void>
	onError?: NoInfer<(input: In, error: Error) => void>
	onSettled?: NoInfer<(input: In, error: Error | null, output: Out | null) => void>
	onTimeout?: NoInfer<(input: In) => void>
	onCancel?: NoInfer<(input: In) => void>
	onRetry?: NoInfer<(input: In, error: Error, attempt: number) => void>
}

type ProgramFn<In extends Data = Data, Out extends Data = Data> = (
	input: In
) => Promise<Out>

interface Program<In extends Data = Data, Out extends Data = Data, Events extends string = never, Id extends string = never> {
	readonly invoke: (input: In) => Promise<Out>
	readonly dispatch: (input: In) => void
	readonly cancel: (input: In) => void
	readonly id: Id
	readonly opts: {}
	readonly __in: In
	readonly __out: Out
	readonly __events: Events
}

const asyncLocalStorage = new AsyncLocalStorage()
const emitter = new EventEmitter()
const interrupt = Symbol('interrupt')
const executables = new Map<string, (input: Data, stepData: Data) => Promise<any>>()
let internalRegistry = new Proxy({}, {
	get() {
		throw new Error('call `registerPrograms` before starting to schedule programs')
	}
}) as Registry2['registry']


declare global {
	interface Registry2 { }
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
	run<T extends Data>(name: string | {
		name: string
		retry?: ProgramRetry
		concurrency?: ConcurrencyOptions
	}, fn: () => Promise<T> | T): Promise<T>
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

const step = {
	run(name, fn) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`run` must be called within a program')
		return store.run(name, fn)
	},
	sleep(ms) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`sleep` must be called within a program')
		return store.sleep(ms)
	},
	dispatchEvent(name, data) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`dispatchEvent` must be called within a program')
		return store.dispatchEvent(name, data)
	},
	waitForEvent(name) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`waitForEvent` must be called within a program')
		return store.waitForEvent(name)
	},
	dispatchProgram(id, input) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`dispatchProgram` must be called within a program')
		return store.dispatchProgram(id, input)
	},
	invokeProgram(id, input) {
		const store = asyncLocalStorage.getStore()
		if (!store) throw new Error('`invokeProgram` must be called within a program')
		return store.invokeProgram(id, input)
	},
} as Utils

const SYSTEM_EVENTS = {
	trigger: 'system/trigger',
	start: 'system/start',
	cancel: 'system/cancel',
	success: 'system/success',
	error: 'system/error',
	settled: 'system/settled',
}

function serialize(obj: Data): string {
	if (!obj || typeof obj !== 'object') return JSON.stringify(obj)
	if (Array.isArray(obj)) return `[${obj.map(serialize).join(',')}]`
	const keys = Object.keys(obj).sort()
	return `{${keys.map((key) => `"${key}":${serialize(obj[key])}`).join(',')}}`
}
function serializeError(error: Error): string {
	const cause = error.cause
	if (cause instanceof Error) {
		return JSON.stringify({
			message: cause.message,
			stack: cause.stack,
			cause: cause.cause ? serializeError(cause) : undefined,

		})
	}
	return JSON.stringify({
		message: error.message,
		stack: error.stack,
	})
}

function createProgram<In extends Data = Data, Out extends Data = Data, Events extends string = never, const Id extends string = never>(
	config: ProgramOptions<In, Out, Events, Id>,
	fn: ProgramFn<In, Out>
): NoInfer<Program<In, Out, Events, Id>> {
	const c: ProgramOptions<In, Out, string, Id> = typeof config === 'string' ? { id: config } : config

	c.triggers ??= {}
	c.triggers.event = typeof c.triggers.event === 'string' ? [c.triggers.event] : c.triggers.event ?? []

	c.retry ??= { attempts: 2, delay: 1000 }
	c.retry.attempts ||= 2

	c.priority ??= 0

	if (typeof c.timings?.concurrency === 'number') {
		c.timings.concurrency = [{
			limit: c.timings.concurrency,
			id: c.id,
			delay: 0,
		}]
	} if (c.timings?.concurrency) {
		if (!Array.isArray(c.timings.concurrency)) c.timings.concurrency = [c.timings.concurrency]
		c.timings.concurrency = c.timings.concurrency.map((concurrency) => ({
			...concurrency,
			id: concurrency.id ?? c.id,
			limit: concurrency.limit ?? 1,
			delay: concurrency.delay ?? 0,
		}))
	}

	const events = {
		/** when the program was just added to the queue */
		trigger: `system/${c.id}/trigger`,
		/** when the program was picked up by the runner */
		start: `system/${c.id}/start`,
		/** when program was still running, but will be terminated (timeout, debounce, programmatic cancellation) */
		cancel: `system/${c.id}/cancel`,
		/** when the program will not continue and reached the end */
		success: `system/${c.id}/success`,
		/** when the program will not continue due to an error */
		error: `system/${c.id}/error`,
		/** when program has nothing to execute anymore */
		settled: `system/${c.id}/settled`,
		/** when something the program was waiting on has occurred, should result in a re-run of the program */
		continue: `system/${c.id}/continue`,
	}

	emitter.on(events.cancel, (data: In) => {
		c.onCancel?.(data)
		const key = serialize(data)
		// match it with the database
		// mark it as cancelled
		emitter.emit(events.settled, data, null, null)
		emitter.emit(SYSTEM_EVENTS.cancel, { id: c.id, in: data })
	})
	emitter.on(events.error, (data: In, error: Error) => {
		c.onError?.(data, error)
		const key = serialize(data)
		const value = serializeError(error)
		// match input to the database
		// mark it as errored, with the error
		emitter.emit(events.settled, data, error, null)
		emitter.emit(SYSTEM_EVENTS.error, { id: c.id, in: data, error: error })
	})
	emitter.on(events.trigger, (data: In) => {
		c.onTrigger?.(data)
		const key = serialize(data)
		// insert into the database
		emitter.emit(SYSTEM_EVENTS.trigger, { id: c.id, in: data })
	})
	emitter.on(events.success, (input: In, out: Out) => {
		c.onSuccess?.(input, out)
		const key = serialize(input)
		const value = serialize(out)
		// match input to the database
		// mark it as successful, with the output
		emitter.emit(events.settled, input, null, out)
		emitter.emit(SYSTEM_EVENTS.success, { id: c.id, in: input, out: out })
	})
	emitter.on(events.start, (data: In) => {
		c.onStart?.(data)
		const key = serialize(data)
		// match it with the database
		// mark it as started
		emitter.emit(SYSTEM_EVENTS.start, { id: c.id, in: data })
	})
	emitter.on(events.settled, (input: In, error: Error | null, output: Out | null) => {
		c.onSettled?.(input, error, output)
		emitter.emit(SYSTEM_EVENTS.settled, { id: c.id, in: input, error, out: output })
	})

	executables.set(c.id, async (input: Data, stepData: Data) => {
		const store = {
			state: {
				id: c.id,
				key: serialize(input),
				data: stepData,
				index: 0,
			},
			run(name, fn) {
				// todo
			},
			sleep(ms) {
				// todo
			},
			dispatchEvent(name, data) {
				// todo
			},
			waitForEvent(name) {
				// todo
			},
			dispatchProgram(idOrProgram, input) {
				// todo
			},
			invokeProgram(idOrProgram, input) {
				// todo
			},
		}
		asyncLocalStorage.run(store, async () => {
			try {
				step.run({ name: 'start', retry: { attempts: 0 } }, () => { emitter.emit(events.start, input) })
				const validIn = c.input ? c.input.parse(input) : input
				const output = await fn(validIn as In)
				const validOut = c.output ? c.output.parse(output) : output
				step.run({ name: 'success', retry: { attempts: 0 } }, () => { emitter.emit(events.success, validIn, validOut) })
			} catch (error) {
				if (error === interrupt) return
				// ignoring retries for now, should they be handled here or in the event listener?
				emitter.emit(
					events.error,
					input,
					new Error(`Runtime error in ${c.id} during/after step ${'not implemented yet'}`, { cause: error })
				)
			}
		})
	})

	function invoke(input: In): Promise<Out> {
		emitter.emit(events.trigger, input)
		return new Promise((resolve, reject) => {
			const key = serialize(input)
			emitter.once(events.settled, (input: In, err: Error | null, output: Out | null) => {
				const match = key === serialize(input)
				if (!match) return
				if (err) return reject(err)
				resolve(output as Out)
			})
		})
	}
	function dispatch(input: In): void {
		emitter.emit(events.trigger, input)
	}
	function cancel(input: In): void {
		emitter.emit(events.cancel, input)
	}
	for (const event of c.triggers.event) {
		emitter.on(event, (data: In) => {
			emitter.emit(events.trigger, data)
		})
	}


	return {
		invoke,
		dispatch,
		cancel,
		id: c.id,
		opts: c,
		__in: {} as In,
		__out: {} as Out,
		__events: {} as Events,
	}
}

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
	internalRegistry = dictionary as any
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
