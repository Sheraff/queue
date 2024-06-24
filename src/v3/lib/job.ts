import EventEmitter from "events"
import type { Data, DeepPartial, InputData, Validator } from "./types"
import { Pipe, type PipeInto } from "./pipe"
import { execution, registration, type ExecutionContext, type RegistrationContext } from "./context"
import type { Step, Task } from "./storage"
import { hydrateError, interrupt, isInterrupt, isPromise, NonRecoverableError, serialize, serializeError } from "./utils"
import parseMs, { type StringValue as DurationString } from 'ms'

type CancelReason =
	| { type: 'timeout', ms: number }
	| { type: 'explicit' }
	| { type: 'debounce', number: number }

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: { input: In }, meta: { input: string }]
	start: [data: { input: In }, meta: { input: string }]
	run: [data: { input: In }, meta: { input: string }]
	success: [data: { input: In, result: Out }, meta: { input: string }]
	error: [data: { input: In, error: unknown }, meta: { input: string }]
	cancel: [data: { input: In, reason: CancelReason }, meta: { input: string }]
	settled: [data: { input: In, result: Out | null, error: unknown | null, reason: CancelReason | null }, meta: { input: string }]
}

const system = Symbol('system')
export type RunOptions = {
	id: string
	[system]?: boolean
	// timeout
	// retries
	// concurrency
	// ...
}

export type WaitForOptions<Filter extends InputData> = {
	filter?: DeepPartial<Filter>
	timeout?: number
	/** Should past events be able to satisfy this request? Defaults to `true`. Use `false` to indicate that only events emitted after this step ran can be used. */
	retroactive?: boolean
}

export const exec = Symbol('exec')
export class Job<
	const Id extends string = string,
	In extends InputData = InputData,
	Out extends Data = Data,
> {
	/** @public */
	readonly id: Id

	/** @package */
	readonly in = null as unknown as In
	/** @package */
	readonly out = null as unknown as Out
	/** @package */
	readonly input: Validator<In> | null
	/** @package */
	readonly output: Validator<Out> | null
	/** @package */
	readonly events = null as unknown as EventMap<In, Out>
	/** @package */
	readonly triggers: NoInfer<Array<Pipe<string, InputData> | PipeInto<any, InputData>>> | undefined
	/** @package */
	readonly cron: string | string[] | undefined

	readonly #emitter = new EventEmitter<EventMap<In, Out>>()
	readonly type = 'job'
	readonly #fn: (input: Data) => Promise<Data>

	/** @public */
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
			triggers?: NoInfer<Array<Pipe<string, In> | PipeInto<any, In>>>
			/** The job must accept a `{date: '<ISO string>'}` input to use a cron schedule (or no input at all). */
			cron?: NoInfer<In extends { date: string } ? string | string[] : InputData extends In ? string | string[] : never>
			onTrigger?: (params: { input: In }) => void
			onStart?: (params: { input: In }) => void
			onSuccess?: (params: { input: In, result: Out }) => void
			onError?: (params: { input: In, error: unknown }) => void
			onCancel?: (params: { input: In, reason: CancelReason }) => void
			onSettled?: (params: { input: In, result: Out | null, error: unknown | null, reason: CancelReason | null }) => void
		},
		fn: (input: In) => Promise<Out>
	) {
		this.id = opts.id
		this.#fn = fn as unknown as (input: Data) => Promise<Data>
		this.input = opts.input ?? null
		this.output = opts.output ?? null
		this.triggers = opts.triggers as Array<Pipe<string, InputData> | PipeInto<any, InputData>>
		this.cron = opts.cron

		this.#emitter.on('trigger', ({ input }, meta) => {
			opts.onTrigger?.({ input })
			const executionContext = execution.getStore()
			if (typeof executionContext === 'object') throw new Error("Cannot call this method inside a job script. Prefer using `Job.dispatch()`, or calling it inside a `Job.run()`.")
			const registrationContext = getRegistrationContext()
			registrationContext.checkRegistration(this)
			registrationContext.recordEvent(`job/${this.id}/trigger`, meta.input, JSON.stringify({ input }))
			registrationContext.addTask(this, input, executionContext, (key, inserted) => {
				if (inserted) return
				registrationContext.queue.storage.getTask(registrationContext.queue.id, this.id, key, (task) => {
					if (!task) throw new Error('Task not found after insert')
					if (task.status === 'failed') {
						if (!task.data) throw new Error('Task previously failed, but no error data found')
						setImmediate(() => this.#emitter.emit('error', { input, error: hydrateError(task.data!) }, meta))
					} else if (task.status === 'completed') {
						if (!task.data) throw new Error('Task previously completed, but no output data found')
						setImmediate(() => this.#emitter.emit('success', { input, result: JSON.parse(task.data!) }, meta))
					} else if (task.status === 'cancelled') {
						if (!task.data) throw new Error('Task previously cancelled, but no reason data found')
						setImmediate(() => this.#emitter.emit('cancel', { input, reason: JSON.parse(task.data!) }, meta))
					}
				})
			})
		})

		this.#emitter.on('start', (input, meta) => {
			opts.onStart?.(input)
			const registrationContext = getRegistrationContext()
			registrationContext.recordEvent(`job/${this.id}/start`, meta.input, JSON.stringify({ input }))
		})

		this.#emitter.on('success', ({ input, result }, meta) => {
			opts.onSuccess?.({ input, result })
			const registrationContext = getRegistrationContext()
			registrationContext.recordEvent(`job/${this.id}/success`, meta.input, JSON.stringify({ input, result }))
			setImmediate(() => this.#emitter.emit('settled', { input, result, error: null, reason: null }, meta))
		})

		this.#emitter.on('error', ({ input, error }, meta) => {
			opts.onError?.({ input, error })
			const registrationContext = getRegistrationContext()
			registrationContext.recordEvent(`job/${this.id}/error`, meta.input, JSON.stringify({ input, error }))
			setImmediate(() => this.#emitter.emit('settled', { input, result: null, error, reason: null }, meta))
		})

		this.#emitter.on('cancel', ({ input }, meta) => {
			opts.onCancel?.({ input, reason: { type: 'explicit' } })
			const registrationContext = getRegistrationContext()
			const reason: CancelReason = { type: 'explicit' } // TODO: add reason
			registrationContext.recordEvent(`job/${this.id}/cancel`, meta.input, JSON.stringify({ input, reason }))
			setImmediate(() => this.#emitter.emit('settled', { input, result: null, error: null, reason }, meta))
		})

		this.#emitter.on('settled', ({ input, result, error, reason }, meta) => {
			opts.onSettled?.({ input, result, error, reason })
			const registrationContext = getRegistrationContext()
			registrationContext.recordEvent(`job/${this.id}/settled`, meta.input, JSON.stringify({ input, result, error, reason }))
		})
	}

	/** @package */
	close(): void {
		this.#emitter.removeAllListeners()
	}

	/** @public */
	dispatch(input: In): void {
		const _input = input ?? {}
		this.#emitter.emit('trigger', { input: _input }, { input: serialize(_input) })
		return
	}

	/** @public */
	static async run<Out extends Data>(id: string, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(opts: RunOptions, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(optsOrId: string | RunOptions, fn: () => Out | Promise<Out>): Promise<Out> {
		const e = getExecutionContext()
		const opts: RunOptions = typeof optsOrId === 'string' ? { id: optsOrId } : optsOrId
		return e.run(opts, fn)
	}

	/** @public */
	static sleep(ms: number | DurationString): Promise<void> {
		const e = getExecutionContext()
		if (typeof ms === 'string') ms = parseMs(ms)
		return e.sleep(ms)
	}

	/** @public */
	static async waitFor<J extends Job, Event extends Exclude<keyof EventMap<J['in'], J['out']>, 'run'>>(job: J, event?: Event, options?: WaitForOptions<J['in']>): Promise<EventMap<J['in'], J['out']>[Event][0]>
	static async waitFor<P extends Pipe<string, any>>(pipe: P, options?: WaitForOptions<P['in']>): Promise<P['in']>
	static async waitFor(instance: Job | Pipe, eventOrOptions?: string | Data, jobOptions?: Data): Promise<Data> {
		const e = getExecutionContext()
		const options = instance instanceof Pipe ? eventOrOptions : jobOptions
		const event = instance instanceof Pipe ? 'success' : eventOrOptions
		return e.waitFor(
			instance,
			(event ?? 'success') as unknown as keyof EventMap<any, any>,
			(options ?? {}) as unknown as WaitForOptions<InputData>
		)
	}

	/** @public */
	static async invoke<J extends Job>(job: J, data: J['in']): Promise<J['out']> {
		const e = getExecutionContext()
		return e.invoke(job, data)
	}

	/** @public */
	static dispatch<I extends Job | Pipe>(instance: I, data: I['in']): void {
		const e = getExecutionContext()
		return e.dispatch(instance, data)
	}

	/** @package */
	[exec](registrationContext: RegistrationContext, task: Task, steps: Step[]): Promise<void> {
		const input = JSON.parse(task.input) as In
		if (!task.started) {
			this.#emitter.emit('start', { input }, { input: task.input })
		} else {
			this.#emitter.emit('run', { input }, { input: task.input })
		}
		const promises: Promise<any>[] = []
		const indexes = {
			system: {} as Record<string, number>,
			user: {} as Record<string, number>
		} as const
		const getIndex = (id: string, system: boolean) => {
			const ind = system ? indexes.system : indexes.user
			const i = ind[id] ?? 0
			ind[id] = i + 1
			return i
		}
		const run: ExecutionContext['run'] = async (options, fn) => {
			const index = getIndex(options.id, options[system] ?? false)
			const step = `${options[system] ? 'system' : 'user'}/${options.id}#${index}`
			const entry = steps.find(s => s.step === step)
			if (entry) {
				if (entry.status === 'completed') {
					if (!entry.data) return
					return JSON.parse(entry.data)
				} else if (entry.status === 'failed') {
					// TODO: handle retries
					if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
					throw hydrateError(entry.data)
				}
			}
			let delegateToNextTick = true
			const canRetry = false // TODO
			let syncResult: Data
			let syncError: unknown
			const onSuccess = (data: Data) => {
				return syncOrPromise<void>(resolve => {
					registrationContext.recordStep(
						task,
						{ step, status: 'completed', data: JSON.stringify(data) },
						resolve
					)
				})
			}
			const onError = (error: unknown) => {
				return syncOrPromise<void>(resolve => {
					registrationContext.recordStep(
						task,
						{ step, status: 'failed', data: serializeError(error) },
						resolve
					)
				})
			}
			try {
				const maybePromise = execution.run(task.id, fn)
				if (isPromise(maybePromise)) {
					promises.push(new Promise<Data>(resolve =>
						registrationContext.recordStep(
							task,
							{ step, status: 'running', data: null },
							() => resolve(maybePromise)
						))
						.then(onSuccess)
						.catch(onError)
					)
				} else {
					const successMaybePromise = onSuccess(maybePromise)
					if (isPromise(successMaybePromise)) {
						promises.push(successMaybePromise)
					} else {
						delegateToNextTick = false
						syncResult = maybePromise
					}
				}
			} catch (err) {
				const errorMaybePromise = onError(err)
				if (isPromise(errorMaybePromise)) {
					promises.push(errorMaybePromise)
				} else {
					delegateToNextTick = canRetry
					syncError = err
				}
			}
			if (delegateToNextTick) {
				await Promise.resolve() // let parallel tasks resolve too
				throw interrupt
			}
			if (syncError) throw syncError
			return syncResult
		}
		const sleep: ExecutionContext['sleep'] = async (ms) => {
			const index = getIndex('sleep', true)
			const step = `system/sleep#${index}`
			const entry = steps.find(s => s.step === step)
			if (entry) {
				if (entry.status === 'completed') return
				if (entry.sleep_done === null) throw new Error('Sleep step already created, but no duration found')
				if (entry.sleep_done) {
					const maybePromise = syncOrPromise<void>(resolve => {
						registrationContext.recordStep(
							task,
							{ step, status: 'completed', data: null, sleep_for: entry.sleep_for },
							resolve
						)
					})
					if (isPromise(maybePromise)) {
						promises.push(maybePromise)
						return maybePromise
					}
					return
				}
				await Promise.resolve()
				throw interrupt
			}
			const maybePromise = syncOrPromise<void>(resolve => {
				registrationContext.recordStep(
					task,
					{ step, status: 'stalled', data: null, sleep_for: ms / 1000 },
					resolve
				)
			})
			if (isPromise(maybePromise)) {
				promises.push(maybePromise)
			}
			await Promise.resolve()
			throw interrupt
		}
		const waitFor: ExecutionContext['waitFor'] = async (instance, event, options) => {
			const name = `waitFor::${instance.type}::${instance.id}::${event}`
			const index = getIndex(name, true)
			const step = `system/${name}#${index}`
			const entry = steps.find(s => s.step === step)

			if (entry) {
				if (entry.status === 'completed') {
					if (!entry.data) return
					return JSON.parse(entry.data)
				} else if (entry.status === 'failed') {
					// TODO: handle retries
					if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
					throw hydrateError(entry.data)
				} else if (entry.status === 'waiting') {
					const maybePromise = syncOrPromise<string>(resolve => {
						registrationContext.resolveEvent(entry, resolve)
					})
					if (isPromise(maybePromise)) {
						promises.push(maybePromise)
						await Promise.resolve()
						throw interrupt
					}
					return JSON.parse(maybePromise as string)
				} else {
					throw new Error(`Unexpected waitFor step status ${entry.status}`)
				}
			}

			const key = instance instanceof Job
				? `job/${instance.id}/${event}`
				: `pipe/${instance.id}`

			const maybePromise = syncOrPromise<void>(resolve => {
				registrationContext.recordStep(
					task,
					{
						step,
						status: 'waiting',
						data: null,
						wait_for: key,
						wait_retroactive: options.retroactive ?? true,
						wait_filter: options.filter ? JSON.stringify(options.filter) : '{}' // TODO: query might be more performant if we supported the null filter case
					},
					resolve
				)
			})
			if (isPromise(maybePromise)) {
				promises.push(maybePromise)
			}
			await Promise.resolve()
			throw interrupt
		}
		const dispatch: ExecutionContext['dispatch'] = (instance, data) => {
			run({
				id: `dispatch-${instance.type}-${instance.id}`,
				[system]: true,
				// retry 0
			}, () => {
				instance.dispatch(data)
			})
		}
		const invoke: ExecutionContext['invoke'] = async (job, input) => {
			const promise = waitFor(job, 'settled', { filter: input })
			dispatch(job, input)
			const { result, error } = (await promise) as { result: Data, error: unknown }
			if (error) throw error
			return result
		}

		const promise = execution.run({ run, sleep, waitFor, invoke, dispatch }, async () => {
			let output: Data
			try {
				let validInput: Data = input
				if (this.input) {
					validInput = await run({
						id: 'parse-input',
						[system]: true,
						// retry 0
					}, () => {
						try {
							return this.input!.parse(input)
						} catch (cause) {
							throw new NonRecoverableError('Input parsing failed', { cause })
						}
					})
				}

				output = await this.#fn(input)

				if (this.output) {
					output = await run({
						id: 'parse-output',
						[system]: true,
						// retry 0
					}, () => {
						try {
							return this.output!.parse(output)
						} catch (cause) {
							throw new NonRecoverableError('Output parsing failed', { cause })
						}
					})
				}
			} catch (error) {
				if (isInterrupt(error)) {
					return Promise.allSettled(promises)
						.then(() => new Promise(setImmediate)) // allow multiple jobs finishing a step on the same tick to continue in priority order
						.then(() => {
							// TODO: handle canceled task
							syncOrPromise<void>(resolve => {
								registrationContext.requeueTask(task, resolve)
							})
						})
				} else {
					// if not cancelled, this is an actual user-land error
					// TODO: handle cancellation
					return syncOrPromise<void>(resolve => {
						registrationContext.resolveTask(task, 'failed', error, resolve)
					}, () => {
						this.#emitter.emit('error', { input, error }, { input: task.input })
					})
				}
			}
			return syncOrPromise<void>(resolve => {
				registrationContext.resolveTask(task, 'completed', output, resolve)
			}, () => {
				this.#emitter.emit('success', { input, result: output as Out }, { input: task.input })
			})
		})

		return promise
	}
}

/**
 * When we run an arbitrary function that takes a callback, and will call it synchronously or asynchronously,
 * this utility will return a promise in the asynchronous case, and the result in the synchronous case.
 * 
 * TODO: there is no error handling
 */
function syncOrPromise<T>(
	fn: (resolver: (arg: T) => void) => void,
	after?: (arg: T) => void
) {
	let sync = false
	let result: T
	const promise = new Promise<T>(resolve => {
		fn((arg) => {
			sync = true
			result = arg
			resolve(arg)
			after?.(arg)
		})
	})
	if (sync) return result! as T
	return promise
}


function getRegistrationContext(): RegistrationContext {
	const context = registration.getStore()
	if (!context) throw new Error("Cannot call this method outside of the context of a queue.")
	return context
}

function getExecutionContext(): ExecutionContext {
	const executionContext = execution.getStore()
	if (typeof executionContext === 'number') throw new Error("Nested job steps are not allowed.")
	if (!executionContext) throw new Error("Cannot call this method outside of a job function.")
	return executionContext
}