import EventEmitter from "events"
import type { Data, DeepPartial, InputData, Validator } from "./types"
import { Pipe, type PipeInto } from "./pipe"
import { execution, type ExecutionContext, type RegistrationContext } from "./context"
import type { Step, Task } from "./storage"
import { getRegistrationContext, hash, hydrateError, interrupt, isInterrupt, isPromise, NonRecoverableError, serialize, serializeError, TimeoutError } from "./utils"
import { parseDuration, parsePeriod, type Duration, type Frequency } from './ms'
import { type ResourceLimits, SHARE_ENV, Worker } from "worker_threads"
import { transferableAbortSignal } from "util"
import { type Logger, system as loggerSystem } from "./logger"

export type CancelReason =
	| { type: 'timeout' }
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
	settled: [data: { input: In, result: Out | null, error: unknown | null, reason: CancelReason | null }, meta: EventMeta & { serializedError?: string }]
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
	backoff?: number | Duration | ((attempt: number) => number | Duration) | number[] | Duration[]
	timeout?: number | Duration
	/*
	 * concurrency is a setting at the step level,
	 * but can be overridden by the content of the step itself.
	 * e.g. to handle 429 errors: if the step fails with a 429, throw a special error that will force a specific delay before retrying.
	 * all steps with the same concurrency id will be delayed by this single error.
	 */
	// TODO: concurrency
	// ...
}

export type ThreadOptions = RunOptions & {
	resourceLimits?: ResourceLimits
}

export type WaitForOptions<Filter extends InputData> = {
	filter?: DeepPartial<Filter>
	timeout?: number | Duration
	/** Should past events be able to satisfy this request? Defaults to `true`. Use `false` to indicate that only events emitted after this step ran can be used. */
	retroactive?: boolean
}

type OrchestrationTimer<In extends Data> = number | Frequency | { id?: string, ms?: number | Frequency } | ((input: In) => number | Frequency | { id?: string, ms?: number | Frequency })

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
	/** @package */
	readonly string: string

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
			priority?: number | ((input: NoInfer<In>) => number)
			/** The job must accept a `{date: '<ISO string>'}` input to use a cron schedule (or no input at all). */
			cron?: NoInfer<In extends { date: string } ? string | string[] : InputData extends In ? string | string[] : never>
			/**
			 * Debounce configuration.
			 * 
			 * A job with a debounce ID will be delayed until the debounce duration has passed.
			 * Any incoming job with the same debounce ID during the debounce period will cancel the previous one, and reset the timer.
			 * 
			 * Accepted configs:
			 * - If it's a value (number or "10 per second"), it will be used as the debounce duration in milliseconds. The debounce ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a value or an object as described above.
			 */
			// TODO: add 'timeout' option to debounce so it doesn't reset the timer forever
			debounce?: NoInfer<OrchestrationTimer<In>>
			/**
			 * Throttle configuration.
			 * 
			 * Any incoming job with the same throttle ID will be delayed until the previous one has completed.
			 * 
			 * Accepted configs:
			 * - If it's a value (number or "10 per second"), it will be used as the throttle duration in milliseconds. The throttle ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a value or an object as described above.
			 */
			throttle?: NoInfer<OrchestrationTimer<In>>
			/**
			 * Rate limit configuration.
			 * 
			 * Any incoming job with the a rate limit ID will be immediately dropped
			 * if another job with the same ID was triggered within the rate limit duration.
			 * It is not recorded in storage, and does not emit any events after the 'trigger'.
			 * 
			 * Accepted configs:
			 * - If it's a value (number or "10 per second"), it will be used as the rate limit duration in milliseconds. The rate limit ID will be the job ID.
			 * - It can be an object with the `id` and `ms` properties
			 * - If it's a function, it will be called with the input data, and should return a value or an object as described above.
			 */
			rateLimit?: NoInfer<OrchestrationTimer<In>>
			/**
			 * Timeout configuration.
			 * 
			 * If the job takes longer than the timeout duration, it will be cancelled with a timeout reason.
			 * 
			 * Accepted configs:
			 * - If it's a value (number or "30 min"), it will be used as the timeout duration in milliseconds.
			 * - If it's a function, it will be called with the input data, and should return a value as described above.
			 * 
			 * @default "1 hour" 
			 */
			timeout?: number | Duration | ((input: NoInfer<In>) => number | Duration)
			// TODO: ttl => if Xms after the trigger the job hasn't started (e.g. because of a throttle), it will be cancelled
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

		this.string = `new Job({ ${Object.entries(opts).map(([key, value]) => {
			if (typeof value === 'function') return `${key}: ${value.toString()}`
			if (key === 'input' || key === 'output') return `${key}: ParserObject`
			return `${key}: ${JSON.stringify(value)}`
		}).join(', ')} }, ${fn.toString()})`

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
				const timeout = resolveJobTimeout(opts.timeout ?? "1 hour", input)

				registrationContext.addTask(this, input, meta.key, executionContext, priority, debounce, throttle, rateLimit, timeout, (rateLimitError, inserted, cancelled) => {
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
								setImmediate(() => this.#emitter.emit('success', { input, result: task.data && JSON.parse(task.data!) }, meta))
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
				const serializedError = serializeError(error)
				registrationContext.recordEvent(`job/${this.id}/error`, meta.input, JSON.stringify({ input, error: serializedError }))
				setImmediate(() => this.#emitter.emit('settled', { input, result: null, error, reason: null }, { ...meta, serializedError }))
			})

			this.#emitter.on('cancel', ({ input, reason }, meta) => {
				opts.onCancel?.({ input, reason })
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
				registrationContext.recordEvent(`job/${this.id}/settled`, meta.input, JSON.stringify({ input, result, error: meta.serializedError ?? null, reason }))
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
		setImmediate(() => {
			this.#emitter.emit('trigger', { input: _input }, { input: serialized, key, queue: registrationContext.queue.id })
		})
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
		setImmediate(() => {
			this.#emitter.emit('cancel', { input: _input, reason }, { input: serialized, key, queue: registrationContext.queue.id })
		})
		return key
	}

	/**
	 * @public
	 * 
	 * @description
	 * The `signal` property of the `utils` object is only provided when the options contain a `timeout`.
	 */
	static async run<Out extends Data>(id: string, fn: (utils: { signal: AbortSignal, logger: Logger }) => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(opts: RunOptions, fn: (utils: { signal: AbortSignal, logger: Logger }) => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(optsOrId: string | RunOptions, fn: (utils: { signal: AbortSignal, logger: Logger }) => Out | Promise<Out>): Promise<Out> {
		const e = getExecutionContext()
		const opts: RunOptions = typeof optsOrId === 'string' ? { id: optsOrId } : optsOrId
		return e.run(opts, fn)
	}

	static async thread<Out extends Data, In extends Data = undefined>(id: string, fn: (input: In, utils: { signal: AbortSignal, logger: Pick<Logger, 'info' | 'warn' | 'error'> }) => Out | Promise<Out>, input?: In): Promise<Out>
	static async thread<Out extends Data, In extends Data = undefined>(opts: ThreadOptions, fn: (input: In, utils: { signal: AbortSignal, logger: Pick<Logger, 'info' | 'warn' | 'error'> }) => Out | Promise<Out>, input?: In): Promise<Out>
	static async thread<Out extends Data, In extends Data = undefined>(optsOrId: string | ThreadOptions, fn: (input: In, utils: { signal: AbortSignal, logger: Pick<Logger, 'info' | 'warn' | 'error'> }) => Out | Promise<Out>, input?: In): Promise<Out> {
		const e = getExecutionContext()
		const opts: RunOptions = typeof optsOrId === 'string' ? { id: optsOrId } : optsOrId
		return e.thread(opts, fn, input)
	}

	/** @public */
	static async sleep(ms: number | Duration): Promise<void> {
		const e = getExecutionContext()
		const duration = ms
		if (typeof ms === 'string') ms = parseDuration(ms)
		return e.sleep(ms, { duration })
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
	static async invoke<J extends Job>(job: J, data: J['in'], options?: Omit<WaitForOptions<J['in']>, "filter" | "retroactive">): Promise<J['out']> {
		const e = getExecutionContext()
		return e.invoke(job, data, options)
	}

	/** @public */
	// TODO: add an option to "schedule for later" (e.g. `dispatch(foo, {}, { delay: "1h" })`)
	static async dispatch<I extends Job | Pipe>(instance: I, data: I['in']): Promise<void> {
		const e = getExecutionContext()
		return e.dispatch(instance, data)
	}

	/** @public */
	static catch(error: unknown) {
		if (isInterrupt(error)) throw error
	}

	/** @public */
	static cancel<I extends Job>(instance: I, data: I['in'], reason: CancelReason): Promise<void> {
		const e = getExecutionContext()
		return e.cancel(instance, data, reason)
	}

	/** @package */
	[exec](registrationContext: RegistrationContext, task: Task, steps: Step[]): Promise<void> {
		const input = JSON.parse(task.input) as In

		if (task.timed_out) {
			this.#emitter.emit('cancel', { input, reason: { type: 'timeout' } }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
			return Promise.resolve()
		}

		const executionContext = makeExecutionContext(registrationContext, task, steps)

		const onCancel: Listener<In, Out, 'cancel'> = (_, { key }) => {
			if (task.key !== key) return
			executionContext.controller.abort()
		}
		this.#emitter.prependListener('cancel', onCancel)
		this.#emitter.setMaxListeners(this.#emitter.getMaxListeners() + 1)

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
					}, {
						source: ''
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
					}, {
						source: ''
					})
				}
			} catch (error) {
				if (isInterrupt(error)) {
					return Promise.allSettled(executionContext.promises)
						.then(() => new Promise(setImmediate)) // allow multiple jobs finishing a step on the same tick to continue in priority order
						.then(() => {
							this.#emitter.off('cancel', onCancel)
							this.#emitter.setMaxListeners(this.#emitter.getMaxListeners() - 1)
							if (executionContext.controller.signal.aborted) return
							syncOrPromise<void>(resolve => {
								registrationContext.requeueTask(task, resolve)
							})
						})
				} else {
					this.#emitter.off('cancel', onCancel)
					this.#emitter.setMaxListeners(this.#emitter.getMaxListeners() - 1)
					if (executionContext.controller.signal.aborted) return
					return syncOrPromise<void>(resolve => {
						registrationContext.resolveTask(task, 'failed', error, resolve)
					}, () => {
						this.#emitter.emit('error', { input, error }, { input: task.input, key: task.key, queue: registrationContext.queue.id })
					})
				}
			}
			this.#emitter.off('cancel', onCancel)
			this.#emitter.setMaxListeners(this.#emitter.getMaxListeners() - 1)
			if (executionContext.controller.signal.aborted) return
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
	const controller = new AbortController()

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

	const run: ExecutionContext['run'] = (options, fn, internals) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		const index = getIndex(options.id, options[system] ?? false)
		const step = `${options[system] ? 'system' : 'user'}/${options.id}#${index}`

		const entry = steps.find(s => s.step === step)
		if (entry) {
			if (entry.status === 'completed') {
				if (!entry.data) return
				return JSON.parse(entry.data)
			}
			if (entry.status === 'failed') {
				if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
				throw hydrateError(entry.data)
			}
			if (entry.status === 'stalled') {
				if (entry.sleep_done === null) throw new Error('Sleep step already created, but no duration found')
				if (!entry.sleep_done) {
					return Promise.reject(interrupt)
				}
			}
		}

		const runs = (entry?.runs ?? 0) + 1
		let delegateToNextTick = true
		let canRetry = false
		let syncResult: Data
		let syncError: unknown
		const source = entry ? null : internals?.source ?? `Job.run("${options.id}", ${fn.toString()})`
		const logger = registrationContext.logger.child({ job: task.job, input: task.input, key: `step/${task.job}/${step}` })

		const onSuccess = (data: Data) => {
			logger[loggerSystem]({ data, runs, event: 'success' })
			return syncOrPromise<void>(resolve => {
				registrationContext.recordStep(
					task,
					{ step, status: 'completed', data: JSON.stringify(data), runs, discovered_on: task.loop, source },
					resolve
				)
			})
		}
		const onError = (error: unknown) => {
			if (controller.signal.aborted) canRetry = false
			else if (error instanceof NonRecoverableError) canRetry = false
			else {
				const retry = options.retry ?? 3
				if (typeof retry === 'number') canRetry = runs < retry
				else canRetry = retry(runs, error)
			}
			const serializedError = serializeError(error)
			logger[loggerSystem]({ error: serializedError, runs, event: 'error' })
			return syncOrPromise<void>(resolve => {
				if (!canRetry) {
					return registrationContext.recordStep(
						task,
						{ step, status: 'failed', data: serializedError, runs, discovered_on: task.loop, source },
						resolve
					)
				}
				const delay = resolveBackoff(options.backoff, runs)
				if (!delay) {
					return registrationContext.recordStep(
						task,
						{ step, status: 'pending', data: null, runs, discovered_on: task.loop, source },
						resolve
					)
				}
				registrationContext.recordStep(
					task,
					{ step, status: 'stalled', data: null, runs, sleep_for: delay / 1000, next_status: 'pending', discovered_on: task.loop, source },
					resolve
				)
			})
		}

		try {
			const runController = new AbortController()
			const utils = {
				signal: runController.signal,
				logger,
			}
			const maybePromise = execution.run(task.id, () => fn(utils))
			if (isPromise(maybePromise)) {
				const timeout = resolveStepTimeout(options.timeout)
				const forwardAbort = () => runController.abort()
				controller.signal.addEventListener('abort', forwardAbort)
				let id: NodeJS.Timeout | null = null
				const promise = timeout !== null
					? Promise.race([
						new Promise<void>((_, reject) =>
							id = setTimeout(() => {
								runController.abort()
								reject(new TimeoutError('Step timed out'))
							}, timeout * 1000)
						),
						maybePromise
					])
					: maybePromise

				logger[loggerSystem]({ runs, event: 'run' })
				promises.push(new Promise<Data>(resolve =>
					registrationContext.recordStep(
						task,
						{ step, status: 'running', data: null, runs, discovered_on: task.loop, source },
						() => resolve(promise)
					))
					.then(onSuccess)
					.catch(onError)
					.finally(() => {
						controller.signal.removeEventListener('abort', forwardAbort)
						if (id) clearTimeout(id)
					})
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
			return Promise.reject(interrupt) // let parallel tasks resolve too
		}
		if (syncError) throw syncError
		return syncResult
	}

	const thread: ExecutionContext['thread'] = (options, fn, input) => {
		const internals = {
			get source() {
				return `Job.thread("${options.id}", ${fn.toString()})`
			}
		}
		return run(options, async ({ signal, logger }) => {
			const workerFn = `
				const { parentPort, workerData } = require('worker_threads')
				try {
					const logger = {
						info: (data) => parentPort.postMessage({ log: ['info', data] }),
						warn: (data) => parentPort.postMessage({ log: ['warn', data] }),
						error: (data) => parentPort.postMessage({ log: ['error', data] }),
					}
					Promise.resolve((${fn.toString()})(workerData.input, { signal: workerData.signal, logger }))
						.then(result => parentPort.postMessage({ result }))
						.catch(err => parentPort.postMessage({ error: err }))
				} catch (error) {
					parentPort.postMessage({ error })
				}
			`
			return new Promise((resolve, reject) => {
				const transferableSignal = transferableAbortSignal(signal)
				const worker = new Worker(workerFn, {
					eval: true,
					workerData: {
						input,
						signal: transferableSignal
					},
					transferList: [
						// @ts-expect-error -- this is transferable
						transferableSignal
					],
					env: SHARE_ENV,
					name: options.id,
					resourceLimits: options.resourceLimits
				})
				worker.on('message', (value) => {
					if ('error' in value) return reject(value.error)
					//@ts-expect-error -- internally annoying to type, externally safe already
					if ('log' in value) return logger[value.log[0]](value.log[1])
					resolve(value.result)
				})
				worker.on('error', (err) => {
					reject(err)
				})
				worker.on('exit', (code) => {
					if (code !== 0)
						reject(new Error(`Worker stopped with exit code ${code}`))
				})
			})
		}, internals)
	}

	const sleep: ExecutionContext['sleep'] = (ms, internals) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		const index = getIndex('sleep', true)
		const step = `system/sleep#${index}`
		const entry = steps.find(s => s.step === step)
		if (entry) {
			if (entry.status === 'completed') return
			if (entry.sleep_done === null) throw new Error('Sleep step already created, but no duration found')
			if (entry.sleep_done) throw new Error('Sleep step already completed')
			if (!entry.sleep_done) {
				return Promise.reject(interrupt)
			}
		}
		const status = ms <= 0 ? 'completed' : 'stalled'
		const source = entry ? null : `Job.sleep(${JSON.stringify(internals?.duration ?? ms)})`
		const maybePromise = syncOrPromise<void>(resolve => {
			registrationContext.recordStep(
				task,
				{ step, status, data: null, sleep_for: ms / 1000, runs: 0, next_status: 'completed', discovered_on: task.loop, source },
				resolve
			)
		})
		if (isPromise(maybePromise)) {
			promises.push(maybePromise)
		}
		return Promise.reject(interrupt)
	}

	const waitFor: ExecutionContext['waitFor'] = (instance, event, options) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		const name = `waitFor::${instance.type}::${instance.id}::${event}`
		const index = getIndex(name, true)
		const step = `system/${name}#${index}`
		const entry = steps.find(s => s.step === step)

		if (entry) {
			if (entry.status === 'completed') {
				if (!entry.data) return
				return JSON.parse(entry.data)
			}
			if (entry.status === 'failed') {
				if (!entry.data) throw new Error('Step marked as failed in storage, but no error data found')
				throw hydrateError(entry.data)
			}
			if (entry.timed_out) {
				throw new TimeoutError('Step timed out')
			}
			if (entry.status === 'waiting') {
				return Promise.reject(interrupt)
			}
			throw new Error(`Unexpected waitFor step status ${entry.status}`)
		}

		const key = instance instanceof Job
			? `job/${instance.id}/${event}`
			: `pipe/${instance.id}`

		const timeout = resolveStepTimeout(options.timeout)
		const source = `Job.waitFor(queue.${instance.type}s["${instance.id}"], ${event}, ${JSON.stringify(options, null, 2)})`

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
					runs: 0,
					timeout,
					discovered_on: task.loop,
					source,
				},
				resolve
			)
		})
		if (isPromise(maybePromise)) {
			promises.push(maybePromise)
		}
		return Promise.reject(interrupt)
	}

	const dispatch: ExecutionContext['dispatch'] = (instance, data, internals) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		return run({
			id: `dispatch-${instance.type}-${instance.id}`,
			[system]: true,
			retry: 0,
		}, () => {
			instance.dispatch(data)
		}, internals ?? {
			get source() {
				return `Job.dispatch(queue.${instance.type}s["${instance.id}"], ${JSON.stringify(data, null, 2)})`
			}
		})
	}

	const invoke: ExecutionContext['invoke'] = async (job, input, options?: Omit<WaitForOptions<InputData>, 'retroactive' | 'filter'>) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		const promise = waitFor(job, 'settled', { ...options, filter: input })
		await dispatch(job, input, {
			get source() {
				return `Job.invoke(queue.jobs["${job.id}"], ${JSON.stringify(input, null, 2)})`
			}
		})
		const { result, error, reason } = (await promise) as { result: Data, error: unknown, reason: CancelReason }
		if (error) throw error
		if (reason) throw new NonRecoverableError(`Job was cancelled "${reason.type}"`)
		return result
	}

	const cancel: ExecutionContext['cancel'] = (instance, data, reason) => {
		if (controller.signal.aborted) {
			return Promise.reject(interrupt)
		}
		return run({
			id: `cancel-${instance.type}-${instance.id}`,
			[system]: true,
			retry: 0
		}, () => {
			instance.cancel(data, reason)
		}, {
			get source() {
				return `Job.cancel(queue.${instance.type}s["${instance.id}"], ${JSON.stringify(data, null, 2)}, ${JSON.stringify(reason)})`
			}
		})
	}

	return {
		run,
		thread,
		sleep,
		waitFor,
		dispatch,
		invoke,
		cancel,
		promises,
		controller,
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
	const value = typeof item === 'string' ? parseDuration(item) : item
	const delay = Math.max(0, value)
	return delay
}

function resolveOrchestrationConfig(config: OrchestrationTimer<any>, id: string, input?: any): { id: string, s: number } {
	if (typeof config === 'function') return resolveOrchestrationConfig(config(input), id)
	if (typeof config === 'number') return { id, s: config / 1000 }
	if (typeof config === 'string') return { id, s: parsePeriod(config) / 1000 }
	const ms = typeof config.ms === 'string' ? parsePeriod(config.ms) : config.ms ?? 0
	return { id: config.id ?? id, s: ms / 1000 }
}

function resolveJobTimeout<In extends Data>(timeout: number | Duration | ((input: NoInfer<In>) => number | Duration), input: In) {
	if (typeof timeout === 'function') return resolveJobTimeout(timeout(input), input)
	if (typeof timeout === 'string') return resolveJobTimeout(parseDuration(timeout), input)
	return timeout <= 0 ? null : timeout / 1000
}

function resolveStepTimeout(timeout?: number | Duration) {
	if (typeof timeout === 'string') return parseDuration(timeout) / 1000
	if (typeof timeout === 'number') return timeout / 1000
	return null
}

const RETRY_TABLE: Duration[] = [
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