import { AsyncLocalStorage } from "node:async_hooks"
import EventEmitter from "node:events"
import { createHash } from 'node:crypto'
import { makeDb, type Storage, type Task } from "./db.js"

type Scalar = string | number | boolean | null

type GenericSerializable = Scalar | undefined | void | GenericSerializable[] | { [key: string]: GenericSerializable }

type Data = GenericSerializable


type ProgramTriggers<Events extends string = never> = {
	event?: Events | Events[]
	cron?: string | string[]
}

type ConcurrencyOptions = {
	/** Globally unique identifier for the concurrency check, defaults to `id` of the program */
	id?: string | ((input: Data) => string)
	/** How many calls to run concurrently. Defaults to `1`. */
	limit?: number
	/** How long to wait before running the next batch of calls. */
	delay?: number
}

type ProgramTimings = {
	/** How long to wait before running the program. If other calls are made before this time, the timer is reset. */
	// TODO: can be implemented with "cancel" + Promise.race("sleep", "waitForEvent")
	debounce?: number | { timeout: number, id?: string | ((input: Data) => string) }
	/** How long before the program is considered to have timed out, and should be cancelled. */
	timeout?: number
	/** How long to wait between each call to the program. If other calls are made before this time, they are dropped. */
	throttle?: number | { timeout: number, id?: string | ((input: Data) => string) }
	/** `input` will be an array of inputs batched over time. */
	batch?: {
		/** How many calls to batch together. */
		size: number
		/** How long to wait before sending a batch, even if it's not full. */
		timeout?: number
		/** Return a group id, only tasks with the same group id are batched together, defaults to `id` of the program. */
		id?: string | ((input: Data) => string)
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
	readonly __register: (emitter: EventEmitter, asyncLocalStorage: AsyncLocalStorage<Store | null>, registry: BaseRegistry, db: Storage) => (input: Data, stepData: Record<string, StepData>, task: Task) => Promise<any>
	readonly __system_events: Record<string, string>
}

declare global {
	interface Registry2 {

	}
}

type GlobalReg = Registry2 extends { registry: BaseRegistry } ? Registry2['registry'] : BaseRegistry

type UnionToIntersection<U> =
	(U extends any ? (x: U) => void : never) extends ((x: infer I) => void) ? I : never

type RegistryEvents = {
	[event in GlobalReg[keyof GlobalReg]['__events'] & string]: UnionToIntersection<{
		[key in keyof GlobalReg]: event extends GlobalReg[key]['__events'] ? GlobalReg[key]['__in'] : never
	}[keyof GlobalReg]>
}

type RegistryPrograms = {
	[id in keyof GlobalReg]: {
		p: GlobalReg[id]
		__in: GlobalReg[id]['__in']
		__out: GlobalReg[id]['__out']
	}
}

type RunOptions = {
	name: string
	retry?: ProgramRetry
	concurrency?: ConcurrencyOptions
}

interface Utils {
	run<T extends Data>(name: string | RunOptions, fn: () => Promise<T> | T): Promise<T>
	sleep(ms: number): Promise<void>
	/** dispatch an event */
	dispatchEvent<Event extends keyof RegistryEvents & string>(name: Event, data: RegistryEvents[Event]): void
	waitForEvent<Event extends keyof RegistryEvents & string>(name: Event | {
		name: Event
		timeout?: number
	}): Promise<RegistryEvents[Event]>
	/**  */
	dispatchProgram<P extends keyof RegistryPrograms>(id: P | RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): void
	invokeProgram<P extends keyof RegistryPrograms>(id: P | RegistryPrograms[P]['p'], input: RegistryPrograms[P]['__in']): Promise<RegistryPrograms[P]['__out']>
}

type Store = {
	run(name: string | RunOptions, fn: () => (Data | Promise<Data>), kind?: string): Promise<Data>
	sleep(ms: number): Promise<void>
	dispatchEvent(name: string, data: Data): void
	waitForEvent(name: string | object): Promise<Data>
	dispatchProgram(id: keyof BaseRegistry | Program, input: any): void
	invokeProgram(id: any, input: any): Promise<any>
}

const storageStorage = new AsyncLocalStorage<AsyncLocalStorage<Store | null>>()

export const step = {
	run(name, fn) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
		if (!store) throw new Error('`run` must be called within a program')
		return store.run(name, fn)
	},
	sleep(ms) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
		if (!store) throw new Error('`sleep` must be called within a program')
		return store.sleep(ms)
	},
	dispatchEvent(name, data) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
		if (!store) throw new Error('`dispatchEvent` must be called within a program')
		return store.dispatchEvent(name, data)
	},
	waitForEvent(name) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
		if (!store) throw new Error('`waitForEvent` must be called within a program')
		return store.waitForEvent(name)
	},
	dispatchProgram(id, input) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
		if (!store) throw new Error('`dispatchProgram` must be called within a program')
		return store.dispatchProgram(id as any, input)
	},
	invokeProgram(id, input) {
		const asyncLocalStorage = storageStorage.getStore()
		const store = asyncLocalStorage?.getStore()
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
	if (obj === undefined) return 'undefined'
	if (!obj || typeof obj !== 'object') return JSON.stringify(obj)
	if (Array.isArray(obj)) return `[${obj.map(serialize).join(',')}]`
	const keys = Object.keys(obj).sort()
	return `{${keys.map((key) => `"${key}":${serialize(obj[key])}`).join(',')}}`
}
function serializeError(error: unknown): string {
	const e = error instanceof Error
		? error
		: new Error(JSON.stringify(error))
	const cause = e.cause
	if (cause instanceof Error) {
		return JSON.stringify({
			message: cause.message,
			stack: cause.stack,
			cause: cause.cause ? serializeError(cause) : undefined,

		})
	}
	return JSON.stringify({
		message: e.message,
		stack: e.stack,
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

const interruptToken = Symbol('interrupt')
type InterruptError = {
	[interruptToken]: true,
}
function isInterrupt(e: any): e is InterruptError {
	return e && typeof e === 'object' && interruptToken in e
}
function interrupt() {
	throw { [interruptToken]: true }
}
export function forwardInterrupt(e: any) {
	if (isInterrupt(e)) throw e
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

	function register(
		emitter: EventEmitter,
		asyncLocalStorage: AsyncLocalStorage<Store | null>,
		registry: BaseRegistry,
		db: Storage
	) {

		const resolveCancel = (data: In) => {
			db.insertOrReplaceTask({
				program: c.id,
				key: hash(data ?? {}),
				input: JSON.stringify(data),
				status: 'cancelled',
			})
			c.onCancel?.(data)
			emitter.emit(events.settled, data, null, null)
			emitter.emit(SYSTEM_EVENTS.cancel, { id: c.id, in: data })
		}
		emitter.on(events.cancel, (data: In) => {
			const key = hash(data ?? {})
			const match = db.getTask({ program: c.id, key })
			if (!match) return
			if (match.status === 'pending' || match.status === 'started' || match.status === 'waiting') {
				resolveCancel(data)
			}

		})
		emitter.on(events.error, (data: In, error: Error) => {
			c.onError?.(data, error)
			const key = hash(data ?? {})
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
			const key = hash(data ?? {})
			// TODO: should we reset the retries / error if trying to re-trigger?
			const priority = typeof c.priority === 'function' ? c.priority(data) : c.priority ?? 0
			db.insertOrIgnoreTask({
				program: c.id,
				key,
				input: JSON.stringify(data),
				status: 'pending',
				priority,
			})
			emitter.emit(SYSTEM_EVENTS.trigger, { id: c.id, in: data })
		})
		emitter.on(events.success, (input: In, out: Out) => {
			c.onSuccess?.(input, out)
			const key = hash(input ?? {})
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
			// const key = hash(data)
			emitter.emit(SYSTEM_EVENTS.start, { id: c.id, in: data })
		})
		emitter.on(events.settled, (input: In, error: Error | null, output: Out | null) => {
			c.onSettled?.(input, error, output)
			emitter.emit(SYSTEM_EVENTS.settled, { id: c.id, in: input, error, out: output })
		})
		emitter.on(events.continue, (data: In) => {
			emitter.emit(SYSTEM_EVENTS.continue, { id: c.id, in: data })
		})

		for (const event of c.triggers!.event!) {
			emitter.on(event, (data: In) => {
				emitter.emit(events.trigger, data)
			})
		}

		return async (input: Data, stepData: Record<string, StepData>, task: Task) => {
			if (c.timings?.timeout) {
				if (task.created_at * 1000 + c.timings.timeout < Date.now()) {
					c.onTimeout?.(input as In)
					emitter.emit(events.cancel, input)
					return
				}
			}
			let index = 0
			let errorStep: string | null = null
			let latestStep: string | null = null
			const key = hash(input)
			const promises: Promise<void>[] = []
			const schedulerData: {
				sleep: number
			}[] = []
			let cancelled = false
			const onCancel = (data: In) => {
				const incoming = hash(data ?? {})
				if (incoming !== key) return
				cancelled = true
			}
			emitter.on(events.cancel, onCancel)
			const store: Store = {
				async run(name, fn, kind = 'run') {
					if (cancelled) interrupt()
					const opts = typeof name === 'string' ? { name } : name
					const n = opts.name
					// identify self using name and store
					const stepKey = `${kind}:${n}:${index}`
					latestStep = stepKey
					index++
					// get self data from store
					const entry = stepData[stepKey]
					const attempts = opts.retry?.attempts ?? 3
					if (entry) {
						if (!entry.error) return Promise.resolve(entry.data)
						const canRetry = entry.runs < attempts
						if (!canRetry) {
							errorStep = stepKey
							return Promise.reject(hydrateError(entry.error))
						}
						const delay = typeof opts.retry?.delay === 'function' ? opts.retry.delay(entry.runs) : opts.retry?.delay ?? 0
						if (delay) {
							const now = Date.now()
							const delta = now - entry.lastRun
							if (delta < delay) {
								// early exit, this task is not ready to re-run yet
								schedulerData.push({ sleep: delay - delta })
								await Promise.resolve()
								interrupt()
							}
						}
					}

					let delegateToNextTick = true

					const onSuccess = (data: Data) => {
						if (cancelled) return
						db.insertOrReplaceMemo({
							program: c.id,
							key,
							step: stepKey,
							status: 'success',
							runs: entry ? entry.runs + 1 : 1,
							last_run: Date.now(),
							data: JSON.stringify(data),
						})
					}
					const run = entry ? entry.runs + 1 : 1
					const canRetry = run < attempts
					const onError = (error: unknown) => {
						if (cancelled) return
						errorStep = stepKey
						db.insertOrReplaceMemo({
							program: c.id,
							key,
							step: stepKey,
							status: 'error',
							runs: run,
							last_run: Date.now(),
							data: serializeError(error),
						})
						if (canRetry && opts.retry?.delay) {
							const delay = typeof opts.retry.delay === 'function' ? opts.retry.delay(run) : opts.retry.delay
							schedulerData.push({ sleep: delay })
						}
					}

					let syncResult: Data
					let syncError: unknown
					try {
						const maybePromise = asyncLocalStorage.run(null, fn)
						if (isPromise(maybePromise)) {
							const promise = maybePromise.then(onSuccess).catch(onError)
							promises.push(promise)
						} else {
							onSuccess(maybePromise)
							delegateToNextTick = false
							syncResult = maybePromise
						}
					} catch (error) {
						onError(error)
						delegateToNextTick = canRetry
						syncError = error
					}

					if (delegateToNextTick) {
						await Promise.resolve() // give other parallel tasks a chance to run before we throw an interrupt
						interrupt()
					}
					if (syncError) throw syncError
					return syncResult
				},
				async sleep(ms) {
					await store.run({ name: 'sleep', retry: { attempts: 0 } }, async () => {
						schedulerData.push({ sleep: ms })
					}, 'system')
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
					const program = (typeof idOrProgram === 'string' ? registry[idOrProgram as keyof typeof registry] : registry[idOrProgram.id]) as Program | undefined
					if (!program) throw new Error(`Program not found`)
					store.run(program.id, () => program.dispatch(input), 'dispatchProgram')
						// dispatch is not meant to be awaited, so we need to catch the interrupt here
						.catch(e => {
							if (!isInterrupt(e)) throw e
						})
				},
				invokeProgram(idOrProgram, input) {
					// todo: we cannot just invoke the other program and return its promise here, programs could take a long time to run
					// we should register a trigger (likely based on "wait for event"), and here we just fire-and-forget the other program
					throw new Error("Method not implemented")
				},
			}
			await storageStorage.run(asyncLocalStorage, () =>
				asyncLocalStorage.run(store, async () => {
					db.insertOrReplaceTask({
						program: c.id,
						key,
						input: JSON.stringify(input),
						status: 'stalled',
					})
					try {
						await store.run({ name: 'start', retry: { attempts: 0 } }, () => { emitter.emit(events.start, input) }, 'system')
						const validIn = c.input ? c.input.parse(input) : input
						const output = await fn(validIn as In)
						const validOut = c.output ? c.output.parse(output) : output
						if (!cancelled) emitter.emit(events.success, validIn, validOut)
						emitter.off(events.cancel, onCancel)
					} catch (error) {
						emitter.off(events.cancel, onCancel)
						if (isInterrupt(error)) {
							emitter.on(events.cancel, onCancel)
							return Promise.all(promises)
								.then(() => new Promise(setImmediate)) // allow multiple programs finishing a step on the same tick to continue in priority order
								.then(() => {
									emitter.off(events.cancel, onCancel)
									if (cancelled) {
										resolveCancel(input as In)
										return
									}
									const sleeps = schedulerData.map(s => s.sleep)
									const sleep = Math.min(...sleeps)
									if (sleeps.length && sleep) {
										db.sleepOrIgnoreTask({
											program: c.id,
											key,
											seconds: sleep / 1000,
										})
									} else {
										db.insertOrReplaceTask({
											program: c.id,
											key,
											input: JSON.stringify(input),
											status: 'started',
										})
									}
									emitter.emit(events.continue, input)
								})
						} else if (!cancelled) {
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
					}
				})
			)
		}
	}

	function unregisteredCall() {
		throw new Error('call `registerPrograms` before starting to schedule programs')
	}

	return {
		invoke: unregisteredCall as any,
		dispatch: unregisteredCall as any,
		cancel: unregisteredCall as any,
		id: c.id,
		opts: c,
		__in: {} as In,
		__out: {} as Out,
		__events: {} as Events,
		__register: register,
		__system_events: events,
	}
}


type BaseRegistry = {
	[Key in string]: Program<any, any, any, Key>['id'] extends Key ? Program<any, any, any, Key> : never
}

type RegEvents<Registry extends BaseRegistry> = {
	[event in Registry[keyof Registry]['__events']]: UnionToIntersection<{
		[key in keyof Registry]: event extends Registry[key]['__events'] ? Registry[key]['__in'] : never
	}[keyof Registry]>
}

type RegPrograms<Registry extends BaseRegistry> = {
	[id in keyof Registry]: {
		p: Registry[id]
		__in: Registry[id]['__in']
		__out: Registry[id]['__out']
	}
}

type RegistryStore<Registry extends BaseRegistry> = {
	run(name: string | object, fn: () => (Data | Promise<Data>), kind?: string): Promise<Data>
	sleep(ms: number): Promise<void>
	dispatchEvent(name: string, data: Data): void
	waitForEvent(name: string | object): Promise<Data>
	dispatchProgram(id: keyof Registry | Program, input: any): void
	invokeProgram(id: any, input: any): Promise<any>
}

type StepData = {
	error: string | null,
	data: Data | null,
	runs: number,
	lastRun: number,
}

export class Queue<const Registry extends BaseRegistry = BaseRegistry> {

	public registry: Registry
	public emitter = new EventEmitter()

	#asyncLocalStorage = new AsyncLocalStorage<RegistryStore<Registry> | null>()
	#executables = new Map<string, (input: Data, stepData: Record<string, StepData>, task: Task) => Promise<any>>()
	#db: Storage

	constructor(
		registry: Registry,
		options: {
			dbName?: string
		} = {}
	) {
		this.#db = makeDb(options.dbName)
		this.registry = {} as Registry
		for (const key in registry) {
			this.#registerProgram(registry[key]!)
		}
		this.#start()
	}

	#registerProgram<In extends Data, Out extends Data>(program: Program<In, Out, any, any>) {
		(this.registry as any)[program.id] = {
			...program,
			invoke: (input: In): Promise<Out> => {
				return new Promise((resolve, reject) => {
					const i = input ?? {}
					const key = hash(i)
					const onSettled = (input: In, err: Error | null, output: Out | null) => {
						const match = key === hash(input ?? {})
						if (!match) return
						this.emitter.off(program.__system_events.settled!, onSettled)
						if (err) return reject(err)
						resolve(output!)
					}
					this.emitter.on(program.__system_events.settled!, onSettled)
					this.emitter.emit(program.__system_events.trigger!, i)
				})
			},
			dispatch: (input: In): void => {
				this.emitter.emit(program.__system_events.trigger!, input ?? {})
			},
			cancel: (input: In): void => {
				this.emitter.emit(program.__system_events.cancel!, input ?? {})
			},
		} as any
		this.#executables.set(program.id, program.__register(this.emitter, this.#asyncLocalStorage, this.registry, this.#db))
	}


	#running = new Set<Promise<any>>()
	#sleepRunTimeout: NodeJS.Timeout | null = null

	#start() {
		let willRun = false
		const execOne = (): 'no-task' | 'ok' => {
			const [task, next] = this.#db.getNextTask()
			if (!task) {
				willRun = false
				return 'no-task'
			}
			const { key, input, program } = task
			const executable = this.#executables.get(program)
			if (!executable) throw new Error(`Program "${program}" not found`)
			const stepData = this.#db.getMemosForTask({ program, key })
			const promise = executable(
				JSON.parse(input),
				stepData.reduce<Record<string, StepData>>((acc, cur) => {
					acc[cur.step] = {
						error: cur.status === 'error' ? cur.data : null,
						data: cur.status === 'success' ? JSON.parse(cur.data!) : null,
						runs: cur.runs,
						lastRun: cur.last_run,
					}
					return acc
				}, {}),
				task
			)
			this.#running.add(promise)
			promise.finally(() => {
				this.#running.delete(promise)
			})
			if (next) {
				return execOne()
			}
			willRun = false
			return 'ok'
		}
		const mightExec = () => {
			if (willRun) return
			willRun = true
			if (this.#sleepRunTimeout) {
				clearTimeout(this.#sleepRunTimeout)
				this.#sleepRunTimeout = null
			}
			setImmediate(() => {
				if (this.#closed) return
				const status = execOne()
				if (status === 'no-task') {
					const future = this.#db.getNextFutureTask()
					if (!future) return
					this.#sleepRunTimeout = setTimeout(mightExec, Math.ceil(future.wait_seconds * 1000) + 1)
				}
			})
		}
		this.emitter.on(SYSTEM_EVENTS.trigger, mightExec)
		this.emitter.on(SYSTEM_EVENTS.continue, mightExec)
		// this.emitter.on(SYSTEM_EVENTS.cancel, mightExec) // re-add this when we have triggers that depend on other programs
		// this.emitter.on(SYSTEM_EVENTS.settled, mightExec) // re-add this when we have triggers that depend on other programs
		mightExec()
		// {
		// 	const emit = this.emitter.emit
		// 	// @ts-expect-error -- temp monkey patch
		// 	this.emitter.emit = (event, ...args) => {
		// 		console.log('emit', event, args)
		// 		emit.apply(this.emitter, [event, ...args])
		// 	}
		// }
	}

	#closed = false
	async close() {
		if (this.#closed) return
		this.#closed = true
		if (this.#sleepRunTimeout) {
			clearTimeout(this.#sleepRunTimeout)
			this.#sleepRunTimeout = null
		}
		await Promise.all(this.#running)
		this.#db.close()
		this.emitter.removeAllListeners()
	}

	run<T extends Data>(name: string | {
		name: string
		retry?: ProgramRetry
		concurrency?: ConcurrencyOptions
	}, fn: () => Promise<T> | T): Promise<T> {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`run` must be called within a program')
		return store.run(name, fn) as Promise<T>
	}

	sleep(ms: number): Promise<void> {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`sleep` must be called within a program')
		return store.sleep(ms)
	}

	/** dispatch an event */
	dispatchEvent<Event extends keyof RegEvents<Registry>>(name: Event, data: RegEvents<Registry>[Event]): void {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`dispatchEvent` must be called within a program')
		return store.dispatchEvent(name, data as Data)
	}

	waitForEvent<Event extends keyof RegEvents<Registry>>(name: Event | {
		name: Event
		timeout?: number
	}): Promise<RegEvents<Registry>[Event]> {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`waitForEvent` must be called within a program')
		return store.waitForEvent(name) as any
	}

	/**  */

	dispatchProgram<P extends keyof RegPrograms<Registry>>(id: P | RegPrograms<Registry>[P]['p'], input: RegPrograms<Registry>[P]['__in']): void {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`dispatchProgram` must be called within a program')
		return store.dispatchProgram(id as any, input)
	}

	invokeProgram<P extends keyof RegPrograms<Registry>>(id: P | RegPrograms<Registry>[P]['p'], input: RegPrograms<Registry>[P]['__in']): Promise<RegPrograms<Registry>[P]['__out']> {
		const store = this.#asyncLocalStorage.getStore()
		if (!store) throw new Error('`invokeProgram` must be called within a program')
		return store.invokeProgram(id, input)
	}

	cancelProgram<P extends keyof RegPrograms<Registry>>(idOrProgram: P | RegPrograms<Registry>[P]['p'], input?: RegPrograms<Registry>[P]['__in']): void {
		const program = (typeof idOrProgram === 'string' ? this.registry[idOrProgram as keyof typeof this.registry] : this.registry[(idOrProgram as Program).id]) as Program | undefined
		if (!program) throw new Error(`Program not found`)
		this.emitter.emit(`program/${program.id}/cancel`, input)
	}
}

