import EventEmitter from "events"
import type { Data, DeepPartial, InputData, Validator } from "./types"
import { Pipe, type PipeInto } from "./pipe"
import { execution, type ExecutionContext, type RegistrationContext } from "./context"
import type { Step, Task } from "./storage"
import { getRegistrationContext, hash, hydrateError, interrupt, isInterrupt, isPromise, NonRecoverableError, serialize, serializeError } from "./utils"
import parseMs, { type StringValue } from 'ms'

export type CancelReason =
	| { type: 'timeout', ms: number }
	| { type: 'explicit' }
	| { type: 'debounce' }

type EventMeta = { input: string, key: string, queue: string }

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: { input: In }, meta: EventMeta]
	start: [data: { input: In }, meta: EventMeta]
	run: [data: { input: In }, meta: EventMeta]
	success: [data: { input: In, result: Out }, meta: EventMeta]
	error: [data: { input: In, error: unknown }, meta: EventMeta]
	cancel: [data: { input: In, reason: CancelReason }, meta: EventMeta]
	settled: [data: { input: In, result: Out | null, error: unknown | null, reason: CancelReason | null }, meta: EventMeta]
}

type Listener<In extends Data, Out extends Data, Event extends keyof EventMap<In, Out>> = (...args: EventMap<In, Out>[Event]) => void

const system = Symbol('system')
export type RunOptions = {
	id: string
	[system]?: boolean
	/**
	 * Number of attempts, including the 1st one. Any number below 1 is equivalent to 1.
	 * 
	 * If it's a function, it will be called with number of times the step has been run already,
	 * and the error that caused the last one to fail.
	 * 
	 * Defaults to 3 attempts.
	 */
	retry?: number | ((attempt: number, error: unknown) => boolean)
	/**
	 * Delay in milliseconds before next attempt.
	 * 
	 * - If it's a function, it will be called with number of times the step has been run already.
	 * - If it's an array, it will be used as a table of delays (using the attempt number as lookup index), with the last one repeating indefinitely.
	 * 
	 * Defaults to a list of delays that increase with each attempt: `"100ms", "30s", "2m", "10m", "30m", "1h", "2h", "12h", "1d"`
	 */
	backoff?: number | StringValue | ((attempt: number) => number | StringValue) | number[] | StringValue[]
	// TODO: timeout
	// TODO: concurrency
	// ...
}

export type WaitForOptions<Filter extends InputData> = {
	filter?: DeepPartial<Filter>
	timeout?: number
	/** Should past events be able to satisfy this request? Defaults to `true`. Use `false` to indicate that only events emitted after this step ran can be used. */
	retroactive?: boolean
	// TODO: debounce
}

type OrchestrationTimer<In extends Data> = number | { id?: string, ms?: number } | ((input: In) => number | { id?: string, ms?: number })

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
			priority?: number | ((input: NoInfer<In>) => number)
			cron?: NoInfer<In extends { date: string } ? string | string[] : InputData extends In ? string | string[] : never>
			/**
			 * Debounce configuration.
			 * 
			 * A job with a debounce ID will be delayed until the debounce duration has passed.
			 * Any incoming job with the same debounce ID during the debounce period will cancel the previous one, and reset the timer.
			 * 
			 * Accepted values:
			 * - If it's a number, it will be used as the debounce duration in milliseconds. The debounce ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a number or an object as described above.
			 */
			debounce?: NoInfer<OrchestrationTimer<In>>
			/**
			 * Throttle configuration.
			 * 
			 * Any incoming job with the same throttle ID will be delayed until the previous one has completed.
			 * 
			 * Accepted values:
			 * - If it's a number, it will be used as the throttle duration in milliseconds. The throttle ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a number or an object as described above.
			 */
			throttle?: NoInfer<OrchestrationTimer<In>>
			/**
			 * Rate limit configuration.
			 * 
			 * Any incoming job with the a rate limit ID will be immediately dropped
			 * if another job with the same ID was triggered within the rate limit duration.
			 * It is not recorded in storage, and does not emit any events after the 'trigger'.
			 * 
			 * Accepted values:
			 * - If it's a number, it will be used as the rate limit duration in milliseconds. The rate limit ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a number or an object as described above.
			 */
			rateLimit?: NoInfer<OrchestrationTimer<In>>
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

		if (this.cron && this.input) {
			try {
				this.input.parse({ date: new Date().toISOString() })
			} catch (error) {
				throw new TypeError(`Job ${this.id} has a cron trigger but its input validator does not accept {date: '<ISO string>'} as an input.`)
			}
		}

		this.start = () => {
			if (this.#started) {
				this.#started++
				return
			}
			this.#started = 1
			this.#emitter.on('trigger', ({ input }, meta) => {
				opts.onTrigger?.({ input })
				const executionContext = execution.getStore()
				if (typeof executionContext === 'object') throw new Error("Cannot call this method inside a job script. Prefer using `Job.dispatch()`, or calling it inside a `Job.run()`.")
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/trigger`, meta.input, JSON.stringify({ input }))

				const priority = typeof opts.priority === 'function' ? opts.priority(input) : opts.priority ?? 0
				const debounce = opts.debounce && resolveOrchestrationConfig(opts.debounce, this.id, input)
				const throttle = opts.throttle && resolveOrchestrationConfig(opts.throttle, this.id, input)
				const rateLimit = opts.rateLimit && resolveOrchestrationConfig(opts.rateLimit, this.id, input)

				registrationContext.addTask(this, input, meta.key, executionContext, priority, debounce, throttle, rateLimit, (rateLimitError, inserted, cancelled) => {
					if (rateLimitError !== null) {
						// TODO: should we do something else here?
						console.warn(`Rate limit reached for group ID ${rateLimit?.id} (on job ${this.id}). Retry in ${rateLimitError}ms`)
						return
					}
					if (!inserted) {
						registrationContext.queue.storage.getTask(registrationContext.queue.id, this.id, meta.key, (task) => {
							if (!task) throw new Error('Task not found after insert') // <- this might not always be an error, for example if the task was not added because of a throttle / rate-limit
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
					}
					if (cancelled) {
						if (!cancelled.data) throw new Error('Task previously cancelled, but no reason data found')
						setImmediate(() => this.queue.jobs[cancelled.job]!.cancel(JSON.parse(cancelled.input), JSON.parse(cancelled.data!)))
					}
				})
			})

			this.#emitter.on('start', (input, meta) => {
				opts.onStart?.(input)
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/start`, meta.input, JSON.stringify({ input }))
			})

			this.#emitter.on('success', ({ input, result }, meta) => {
				opts.onSuccess?.({ input, result })
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/success`, meta.input, JSON.stringify({ input, result }))
				setImmediate(() => this.#emitter.emit('settled', { input, result, error: null, reason: null }, meta))
			})

			this.#emitter.on('error', ({ input, error }, meta) => {
				opts.onError?.({ input, error })
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/error`, meta.input, JSON.stringify({ input, error }))
				setImmediate(() => this.#emitter.emit('settled', { input, result: null, error, reason: null }, meta))
			})

			this.#emitter.on('cancel', ({ input, reason }, meta) => {
				opts.onCancel?.({ input, reason: { type: 'explicit' } })
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/cancel`, meta.input, JSON.stringify({ input, reason }))
				// TODO: Update steps too? (to avoid leaving them in a state that would block stuff like concurrency)
				// TODO: find a way to avoid re-cancelling a task that was already cancelled (e.g. by a trigger with the same key, or by a debounce)
				registrationContext.resolveTask({
					queue: registrationContext.queue.id,
					job: this.id,
					key: meta.key,
				}, 'cancelled', JSON.stringify(reason), () => {
					setImmediate(() => this.#emitter.emit('settled', { input, result: null, error: null, reason }, meta))
				})
			})

			this.#emitter.on('settled', ({ input, result, error, reason }, meta) => {
				opts.onSettled?.({ input, result, error, reason })
				const registrationContext = getRegistrationContext(this)
				registrationContext.recordEvent(`job/${this.id}/settled`, meta.input, JSON.stringify({ input, result, error, reason }))
			})
		}
	}

	#started = 0
	/** @package */
	start: () => void
	/** @package */
	close(): void {
		this.#started--
		if (this.#started > 0) return
		this.#emitter.removeAllListeners()
	}

	/**
	 * @public
	 * 
	 * Getter for the parent `queue` in the current context.
	 * 
	 * ```ts
	 * myQueue.jobs.myJob.queue === myQueue
	 * ```
	 * 
	 * @throws {ReferenceError} Will throw an error if called outside of a queue context.
	 */
	get queue() {
		return getRegistrationContext(this).queue
	}

	/** @public */
	dispatch(input: In): string {
		const _input = input ?? {}
		const serialized = serialize(_input)
		const key = hash(serialized)
		const registrationContext = getRegistrationContext(this)
		this.#emitter.emit('trigger', { input: _input }, { input: serialized, key, queue: registrationContext.queue.id })
		return key
	}

	/** @public */
	cancel(input: In, reason: CancelReason): string {
		// TODO: it feels weird to let the user set a reason, since user-land cancellation should be 'explicit' by definition
		// but also, it's handy to be able to call this internally with a reason
		// Technically we could `emit` directly internally? (And make the same change for `dispatch` too)
		const _input = input ?? {}
		const serialized = serialize(_input)
		const key = hash(serialized)
		const registrationContext = getRegistrationContext(this)
		this.#emitter.emit('cancel', { input: _input, reason }, { input: serialized, key, queue: registrationContext.queue.id })
		return key
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
	static sleep(ms: number | StringValue): Promise<void> {
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
	static dispatch<I extends Job | Pipe>(instance: I, data: I['in']): Promise<void> {
		const e = getExecutionContext()
		return e.dispatch(instance, data)
	}

	/** @public */
	static cancel<I extends Job>(instance: I, data: I['in'], reason: CancelReason): Promise<void> {
		const e = getExecutionContext()
		return e.cancel(instance, data, reason)
	}

	/** @package */
	[exec](registrationContext: RegistrationContext, task: Task, steps: Step[]): Promise<void> {
		const input = JSON.parse(task.input) as In

		const executionContext = makeExecutionContext(registrationContext, task, steps)

		const onCancel: Listener<In, Out, 'cancel'> = (_, { key }) => {
			if (task.key !== key) return
			executionContext.cancelled = true
		}
		this.#emitter.prependListener('cancel', onCancel)

		const promise = execution.run(executionContext, async () => {
			if (!task.started_at) {
				this.#emitter.emit('start', { input }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
			} else {
				this.#emitter.emit('run', { input }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
			}

			let output: Data

			try {
				let validInput: Data = input
				if (this.input) {
					validInput = await executionContext.run({
						id: 'parse-input',
						[system]: true,
						retry: 0
					}, () => {
						try {
							return this.input!.parse(input)
						} catch (cause) {
							throw new NonRecoverableError('Input parsing failed', { cause })
						}
					})
				}

				output = await this.#fn(validInput)

				if (this.output) {
					output = await executionContext.run({
						id: 'parse-output',
						[system]: true,
						retry: 0
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
					return Promise.allSettled(executionContext.promises)
						.then(() => new Promise(setImmediate)) // allow multiple jobs finishing a step on the same tick to continue in priority order
						.then(() => {
							this.#emitter.off('cancel', onCancel)
							if (executionContext.cancelled) return
							syncOrPromise<void>(resolve => {
								registrationContext.requeueTask(task, resolve)
							})
						})
				} else {
					this.#emitter.off('cancel', onCancel)
					if (executionContext.cancelled) return
					return syncOrPromise<void>(resolve => {
						registrationContext.resolveTask(task, 'failed', error, resolve)
					}, () => {
						this.#emitter.emit('error', { input, error }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
					})
				}
			}
			this.#emitter.off('cancel', onCancel)
			if (executionContext.cancelled) return
			return syncOrPromise<void>(resolve => {
				registrationContext.resolveTask(task, 'completed', output, resolve)
			}, () => {
				this.#emitter.emit('success', { input, result: output as Out }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
			})
		})

		return promise
	}
}

function makeExecutionContext(registrationContext: RegistrationContext, task: Task, steps: Step[]): ExecutionContext {
	let cancelled = false

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
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		const index = getIndex(options.id, options[system] ?? false)
		const step = `${options[system] ? 'system' : 'user'}/${options.id}#${index}`
		const entry = steps.find(s => s.step === step)
		if (entry) {
			if (entry.status === 'completed') {
				if (!entry.data) return
				return JSON.parse(entry.data)
			} else if (entry.status === 'failed') {
				if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
				throw hydrateError(entry.data)
			} else if (entry.status === 'stalled') {
				if (entry.sleep_done === null) throw new Error('Sleep step already created, but no duration found')
				if (!entry.sleep_done) {
					await Promise.resolve()
					throw interrupt
				}
			}
		}

		const runs = (entry?.runs ?? 0) + 1
		let delegateToNextTick = true
		let canRetry = false
		let syncResult: Data
		let syncError: unknown

		const onSuccess = (data: Data) => {
			return syncOrPromise<void>(resolve => {
				registrationContext.recordStep(
					task,
					{ step, status: 'completed', data: JSON.stringify(data), runs },
					resolve
				)
			})
		}
		const onError = (error: unknown) => {
			if (cancelled) canRetry = false
			else if (error instanceof NonRecoverableError) canRetry = false
			else {
				const retry = options.retry ?? 3
				if (typeof retry === 'number') canRetry = runs < retry
				else canRetry = retry(runs, error)
			}
			return syncOrPromise<void>(resolve => {
				if (!canRetry) {
					return registrationContext.recordStep(
						task,
						{ step, status: 'failed', data: serializeError(error), runs },
						resolve
					)
				}
				const delay = resolveBackoff(options.backoff, runs)
				if (!delay) {
					return registrationContext.recordStep(
						task,
						{ step, status: 'pending', data: null, runs },
						resolve
					)
				}
				registrationContext.recordStep(
					task,
					{ step, status: 'stalled', data: null, runs, sleep_for: delay / 1000, next_status: 'pending' },
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
						{ step, status: 'running', data: null, runs },
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
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		const index = getIndex('sleep', true)
		const step = `system/sleep#${index}`
		const entry = steps.find(s => s.step === step)
		if (entry) {
			if (entry.status === 'completed') return
			if (entry.sleep_done === null) throw new Error('Sleep step already created, but no duration found')
			if (entry.sleep_done) throw new Error('Sleep step already completed')
			if (!entry.sleep_done) {
				await Promise.resolve()
				throw interrupt
			}
		}
		const status = ms <= 0 ? 'completed' : 'stalled'
		const maybePromise = syncOrPromise<void>(resolve => {
			registrationContext.recordStep(
				task,
				{ step, status, data: null, sleep_for: ms / 1000, runs: 0, next_status: 'completed' },
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
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		const name = `waitFor::${instance.type}::${instance.id}::${event}`
		const index = getIndex(name, true)
		const step = `system/${name}#${index}`
		const entry = steps.find(s => s.step === step)

		if (entry) {
			if (entry.status === 'completed') {
				if (!entry.data) return
				return JSON.parse(entry.data)
			} else if (entry.status === 'failed') {
				if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
				throw hydrateError(entry.data)
			} else if (entry.status === 'waiting') {
				await Promise.resolve()
				throw interrupt
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
					wait_filter: options.filter ? JSON.stringify(options.filter) : '{}', // TODO: query might be more performant if we supported the null filter case
					runs: 0
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

	const dispatch: ExecutionContext['dispatch'] = async (instance, data) => {
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		run({
			id: `dispatch-${instance.type}-${instance.id}`,
			[system]: true,
			retry: 0,
		}, () => {
			instance.dispatch(data)
		})
	}

	const invoke: ExecutionContext['invoke'] = async (job, input) => {
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		const promise = waitFor(job, 'settled', { filter: input })
		await dispatch(job, input)
		const { result, error } = (await promise) as { result: Data, error: unknown }
		if (error) throw error
		return result
	}

	const cancel: ExecutionContext['cancel'] = async (instance, data, reason) => {
		if (cancelled) {
			await Promise.resolve()
			throw interrupt
		}
		run({
			id: `cancel-${instance.type}-${instance.id}`,
			[system]: true,
			retry: 0
		}, () => {
			instance.cancel(data, reason)
		})
	}

	return {
		run,
		sleep,
		waitFor,
		dispatch,
		invoke,
		cancel,
		promises,
		get cancelled() { return cancelled },
		set cancelled(value) { cancelled = value },
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

function getExecutionContext(): ExecutionContext {
	const executionContext = execution.getStore()
	if (typeof executionContext === 'number') throw new Error("Nested job steps are not allowed.")
	if (!executionContext) throw new Error("Cannot call this method outside of a job function.")
	return executionContext
}

function resolveBackoff(backoff: RunOptions['backoff'], runs: number) {
	const called = typeof backoff === 'function' ? backoff(runs) : backoff ?? RETRY_TABLE
	const item = Array.isArray(called) ? (called[runs - 1] ?? called.at(-1) ?? 100) : called
	const value = typeof item === 'string' ? parseMs(item) : item
	const delay = Math.max(0, value)
	return delay
}

function resolveOrchestrationConfig(config: OrchestrationTimer<any>, id: string, input?: any) {
	if (typeof config === 'function') return resolveOrchestrationConfig(config(input), id)
	if (typeof config === 'number') return { id, s: config / 1000 }
	return { id: config.id ?? id, s: (config.ms ?? 0) / 1000 }
}

const RETRY_TABLE: StringValue[] = [
	"100ms",
	"30s",
	"2m",
	"10m",
	"30m",
	"1h",
	"2h",
	"12h",
	"1d",
]