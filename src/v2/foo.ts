import { AsyncLocalStorage } from "node:async_hooks"
import EventEmitter from "node:events"
import { createHash } from 'node:crypto'
import { makeDb } from "./db.js"

const db = makeDb('woop.db')

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

export interface Program<In extends Data = Data, Out extends Data = Data, Events extends string = never, Id extends string = never> {
	readonly invoke: (input: In) => Promise<Out>
	readonly dispatch: (input: In) => void
	readonly cancel: (input: In) => void
	readonly id: Id
	readonly opts: {}
	readonly __in: In
	readonly __out: Out
	readonly __events: Events
}

const asyncLocalStorage = new AsyncLocalStorage<Store | null>()
const emitter = new EventEmitter()
{
	const emit = emitter.emit
	// @ts-expect-error -- temp monkey patch
	emitter.emit = (event, ...args) => {
		console.log('emit', event, args)
		emit.apply(emitter, [event, ...args])
	}
}
const interrupt = Symbol('interrupt')
const executables = new Map<string, (input: Data, stepData: Record<string, { error: string | null, data: Data }>) => Promise<any>>()
let internalRegistry = new Proxy({}, {
	get() {
		throw new Error('call `registerPrograms` before starting to schedule programs')
	}
}) as Registry2['registry']

declare global {
	interface Registry2 {

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
	run<T extends Data>(name: string | {
		name: string
		retry?: ProgramRetry
		concurrency?: ConcurrencyOptions
	}, fn: () => Promise<T> | T): Promise<T>
	sleep(ms: number): Promise<void>
	/** dispatch an event */
	dispatchEvent<Event extends keyof RegistryEvents>(name: Event, data: RegistryEvents[Event]): void
	waitForEvent<Event extends keyof RegistryEvents>(name: Event | {
		name: Event
		timeout?: number
	}): Promise<RegistryEvents[Event]>
	/**  */
	dispatchProgram<P extends keyof RegistryPrograms>(id: P | RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): void
	invokeProgram<P extends keyof RegistryPrograms>(id: P | RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): Promise<RegistryPrograms[P]['__out']>
}

type Store = {
	run(name: string | object, fn: () => (Data | Promise<Data>), kind?: string): Promise<Data>
	sleep(ms: number): Promise<void>
	dispatchEvent(name: string, data: Data): void
	waitForEvent(name: string | object): Promise<Data>
	dispatchProgram(id: keyof Registry2['registry'] | Program, input: any): void
	invokeProgram(id: any, input: any): Promise<any>
}

export const step = {
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
		return store.dispatchProgram(id as any, input)
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
	continue: 'system/continue',
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
function hydrateError(serialized: string): Error {
	const obj = JSON.parse(serialized)
	const error = new Error(obj.message)
	error.stack = obj.stack
	if (obj.cause) error.cause = hydrateError(obj.cause)
	return error

}
function isPromise(obj: unknown): obj is Promise<any> {
	return !!obj && typeof obj === 'object' && 'then' in obj && typeof obj.then === 'function'
}
function md5(input: string): string {
	return createHash('md5').update(Buffer.from(input)).digest('hex')
}
function hash(input: Data) {
	const string = serialize(input)
	if (string.length < 40) return string
	return md5(string)
}

export function createProgram<In extends Data = Data, Out extends Data = Data, Events extends string = never, const Id extends string = never>(
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
		trigger: `program/${c.id}/trigger`,
		/** when the program was picked up by the runner */
		start: `program/${c.id}/start`,
		/** when program was still running, but will be terminated (timeout, debounce, programmatic cancellation) */
		cancel: `program/${c.id}/cancel`,
		/** when the program will not continue and reached the end */
		success: `program/${c.id}/success`,
		/** when the program will not continue due to an error */
		error: `program/${c.id}/error`,
		/** when program has nothing to execute anymore */
		settled: `program/${c.id}/settled`,
		/** when something the program was waiting on has occurred, should result in a re-run of the program */
		continue: `program/${c.id}/continue`,
	}

	emitter.on(events.cancel, (data: In) => {
		c.onCancel?.(data)
		const key = hash(data)
		// match it with the database
		// mark it as cancelled
		emitter.emit(events.settled, data, null, null)
		emitter.emit(SYSTEM_EVENTS.cancel, { id: c.id, in: data })
	})
	emitter.on(events.error, (data: In, error: Error) => {
		c.onError?.(data, error)
		const key = hash(data)
		const value = serializeError(error)
		db.insertOrReplaceTask({
			program: c.id,
			key,
			input: JSON.stringify(data),
			status: 'error',
			data: value,
		})
		emitter.emit(events.settled, data, error, null)
		emitter.emit(SYSTEM_EVENTS.error, { id: c.id, in: data, error: error })
	})
	emitter.on(events.trigger, (data: In) => {
		c.onTrigger?.(data)
		const key = hash(data)
		// TODO: should we reset the retries / error if trying to re-trigger?
		db.insertOrIgnoreTask({
			program: c.id,
			key,
			input: JSON.stringify(data),
			status: 'pending',
		})
		emitter.emit(SYSTEM_EVENTS.trigger, { id: c.id, in: data })
	})
	emitter.on(events.success, (input: In, out: Out) => {
		c.onSuccess?.(input, out)
		const key = hash(input)
		db.insertOrReplaceTask({
			program: c.id,
			key,
			input: JSON.stringify(input),
			status: 'success',
			data: JSON.stringify(out),
		})
		emitter.emit(events.settled, input, null, out)
		emitter.emit(SYSTEM_EVENTS.success, { id: c.id, in: input, out: out })
	})
	emitter.on(events.start, (data: In) => {
		c.onStart?.(data)
		const key = hash(data)
		db.insertOrReplaceTask({
			program: c.id,
			key,
			input: JSON.stringify(data),
			status: 'started',
		})
		emitter.emit(SYSTEM_EVENTS.start, { id: c.id, in: data })
	})
	emitter.on(events.settled, (input: In, error: Error | null, output: Out | null) => {
		c.onSettled?.(input, error, output)
		emitter.emit(SYSTEM_EVENTS.settled, { id: c.id, in: input, error, out: output })
	})
	emitter.on(events.continue, (data: In) => {
		emitter.emit(SYSTEM_EVENTS.continue, { id: c.id, in: data })
	})

	executables.set(c.id, async (input: Data, stepData: Record<string, { error: string | null, data: Data | null }>) => {
		let index = 0
		let errorStep: string | null = null
		let latestStep: string | null = null
		const key = hash(input)
		const promises: Promise<any>[] = []
		const store: Store = {
			// state: {
			// 	id: c.id,
			// 	key: serialize(input),
			// 	data: stepData,
			// 	index: 0,
			// },
			async run(name, fn, kind = 'run') {
				// @ts-expect-error -- we need to implement "first param is string or object"
				const n = typeof name === 'string' ? name : name.name
				// identify self using name and store
				const stepKey = `${kind}:${n}:${index}`
				latestStep = stepKey
				index++
				// get self data from store
				const entry = stepData[stepKey]
				if (entry) {
					if (entry.error) {
						errorStep = stepKey
						return Promise.reject(hydrateError(entry.error))
					}
					return Promise.resolve(entry.data)
				}

				// TODO: in v2, check if the result of `fn()` is a promise, if not we could just continue execution
				const promise = Promise.resolve(asyncLocalStorage.run(null, fn))
					.then((data) => {
						db.insertOrReplaceMemo({
							program: c.id,
							key,
							step: stepKey,
							status: 'success',
							data: JSON.stringify(data),
						})
					})
					.catch((error) => {
						errorStep = stepKey
						// store result in store
						db.insertOrReplaceMemo({
							program: c.id,
							key,
							step: stepKey,
							status: 'error',
							data: serializeError(error),
						})
					})

				promises.push(promise)
				await Promise.resolve()
				throw interrupt
			},
			sleep(ms) {
				// todo
				throw new Error("Method not implemented")

			},
			dispatchEvent(name, data) {
				// todo
				throw new Error("Method not implemented")
			},
			waitForEvent(name) {
				// todo
				throw new Error("Method not implemented")
			},
			dispatchProgram(idOrProgram, input) {
				const program = typeof idOrProgram === 'string' ? internalRegistry[idOrProgram as keyof Registry2['registry']] : idOrProgram
				store.run(program.id, () => program.dispatch(input), 'dispatchProgram')
					// dispatch is not meant to be awaited, so we need to catch the interrupt here
					.catch(e => {
						if (e !== interrupt) throw e
					})
			},
			invokeProgram(idOrProgram, input) {
				const program = typeof idOrProgram === 'string' ? internalRegistry[idOrProgram as keyof Registry2['registry']] : idOrProgram
				return store.run(program.id, () => program.invoke(input), 'invokeProgram')
			},
		}
		asyncLocalStorage.run(store, async () => {
			try {
				await step.run({ name: 'system-start', retry: { attempts: 0 } }, () => { emitter.emit(events.start, input) })
				const validIn = c.input ? c.input.parse(input) : input
				const output = await fn(validIn as In)
				const validOut = c.output ? c.output.parse(output) : output
				emitter.emit(events.success, validIn, validOut)
			} catch (error) {
				if (error === interrupt) {
					Promise.all(promises).finally(() => {
						emitter.emit(events.continue, input)
					})
					return
				}
				// ignoring retries for now, should they be handled here or in the event listener?
				emitter.emit(
					events.error,
					input,
					new Error(errorStep
						? `Runtime error in "${c.id}" during step "${errorStep}"`
						: `Runtime error in "${c.id}" after step "${latestStep}"`,
						{ cause: error }
					)
				)
			}
		})
	})

	function invoke(input: In): Promise<Out> {
		return new Promise((resolve, reject) => {
			const key = hash(input)
			emitter.once(events.settled, (input: In, err: Error | null, output: Out | null) => {
				const match = key === hash(input)
				if (!match) return
				if (err) return reject(err)
				resolve(output as Out)
			})
			emitter.emit(events.trigger, input)
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

function execOne() {
	const task = db.getNextTask()
	if (!task) return
	const { key, input, program } = task
	const executable = executables.get(program)
	if (!executable) return
	const stepData = db.getMemosForTask({ program, key })
	executable(
		JSON.parse(input),
		stepData.reduce((acc, cur) => {
			acc[cur.step] = {
				error: cur.status === 'error' ? cur.data : null,
				data: cur.status === 'success' ? JSON.parse(cur.data!) : null
			}
			return acc
		}, {} as Record<string, { error: string | null, data: Data | null }>)
	)
}
emitter.on(SYSTEM_EVENTS.trigger, execOne)
emitter.on(SYSTEM_EVENTS.continue, execOne)

export function registerPrograms<
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


