import EventEmitter from "events"
import type { Data, DeepPartial, Validator } from "./types"
import { Pipe } from "./pipe"
import { execution, registration, type ExecutionContext } from "./context"
import type { Queue } from "./queue"
import type { Step, Task } from "./storage"
import { interrupt, isInterrupt, isPromise, NonRecoverableError } from "./utils"

type EventMap<In extends Data, Out extends Data> = {
	trigger: [data: In]
	start: [data: In]
	success: [data: In, result: Out]
	error: [data: In, error: unknown]
	settled: [data: In, result: Out | null, error: unknown | null]
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

export type WaitForOptions<In extends Data> = {
	filter?: DeepPartial<In>
	timeout?: number
	retroactive?: boolean
}

export const exec = Symbol('exec')
export class Job<
	const Id extends string = string,
	In extends Data = Data,
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
			triggers?: NoInfer<Array<Pipe<string, In>>>
			cron?: string | string[]
		},
		fn: (input: In) => Promise<Out>
	) {
		this.id = opts.id
		this.#fn = fn as unknown as (input: Data) => Promise<Data>
		this.input = opts.input ?? null
		this.output = opts.output ?? null

		this.#emitter.on('trigger', async (data) => {
			const executionContext = execution.getStore()
			if (executionContext) throw new Error("Cannot call this method inside a job script. Prefer using `Job.dispatch()`, or calling it inside a `Job.run()`.")
			const registrationContext = registration.getStore()
			if (!registrationContext) throw new Error("Cannot call this method outside of the context of a queue.")
			registrationContext.checkRegistration(this)
			registrationContext.addTask(this, data)
		})
	}

	/** @package */
	close(): void {
		this.#emitter.removeAllListeners()
	}

	/** @public */
	dispatch(data: In): void {
		this.#emitter.emit('trigger', data)
		return
	}

	/** @public */
	static async run<Out extends Data>(id: string, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(opts: RunOptions, fn: () => Out | Promise<Out>): Promise<Out>
	static async run<Out extends Data>(optsOrId: string | RunOptions, fn: () => Out | Promise<Out>): Promise<Out> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		const opts: RunOptions = typeof optsOrId === 'string' ? { id: optsOrId } : optsOrId
		return e.run(opts, fn)
	}

	/** @public */
	static sleep(ms: number): Promise<void> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.sleep(ms)
	}

	/** @public */
	static async waitFor<J extends Job, Event extends keyof EventMap<J['in'], J['out']>>(job: J, event?: Event, options?: WaitForOptions<J['in']>): Promise<EventMap<J['in'], J['out']>[Event]>
	static async waitFor<P extends Pipe>(pipe: P, options?: WaitForOptions<P['in']>): Promise<P['in']>
	static async waitFor(instance: Job | Pipe, eventOrOptions?: string | Data, jobOptions?: Data): Promise<Data> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		const options = instance instanceof Pipe ? eventOrOptions : jobOptions
		const event = instance instanceof Pipe ? 'success' : eventOrOptions ?? 'success'
		return e.waitFor(
			instance,
			event as unknown as keyof EventMap<any, any>,
			options as unknown as WaitForOptions<Data>
		)
	}

	/** @public */
	static async invoke<J extends Job>(job: J, data: J['in']): Promise<J['out']> {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.invoke(job, data)
	}

	/** @public */
	static dispatch<I extends Job | Pipe>(instance: I, data: I['in']): void {
		const e = execution.getStore()
		if (e === null) throw new Error("Nested job steps are not allowed.")
		if (!e) throw new Error("Cannot call this method outside of a job function.")
		return e.dispatch(instance, data)
	}

	/** @package */
	[exec](queue: Queue, task: Task, steps: Step[]): Promise<void> {
		const promises: Promise<void>[] = []
		let index = 0
		const run: ExecutionContext['run'] = async (options, fn) => {
			const i = index // TODO: would it be better if `index` wasn't "global" to the entire script, but just for each "list of identical steps"?
			index += 1
			const entry = steps.find(s => s.step === options.id && s.index === i && (!options[system] || s.system))
			if (entry && entry.status === 'success') {
				if (!entry.data) return Promise.resolve()
				return Promise.resolve(JSON.parse(entry.data))
			}
			let delegateToNextTick = true
			const canRetry = false // TODO
			let syncResult: Data
			let syncError: unknown
			const onSuccess = (data: Data) => { }
			const onError = (error: unknown) => { }
			try {
				const maybePromise = execution.run(null, fn)
				if (isPromise(maybePromise)) {
					const promise = maybePromise.then(onSuccess).catch(onError)
					promises.push(promise)
				} else {
					const result = maybePromise
					onSuccess(result)
					delegateToNextTick = false
					syncResult = result
				}
			} catch (err) {
				onError(err)
				delegateToNextTick = canRetry
				syncError = err
			}
			if (delegateToNextTick) {
				await Promise.resolve() // let parallel tasks resolve too
				interrupt()
			}
			if (syncError) throw syncError
			return syncResult
		}
		const sleep: ExecutionContext['sleep'] = async (ms) => {
			return
		}
		const waitFor: ExecutionContext['waitFor'] = async (instance, event, options) => {
			return {} as any
		}
		const invoke: ExecutionContext['invoke'] = async (job, data) => {
			return {} as any
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

		const promise = execution.run({ run, sleep, waitFor, invoke, dispatch }, async () => {
			try {
				const input: Data = await run({
					id: 'start',
					[system]: true,
					// retry 0
				}, () => {
					try {
						const input = JSON.parse(task.input)
						if (!this.input) return input
						return this.input.parse(input)
					} catch (cause) {
						throw new NonRecoverableError('Input parsing failed', { cause })
					}
				})

				let output = await this.#fn(input)
				if (this.output) output = this.output.parse(output)
				// dispatch 'success' event w/ output
			} catch (err) {
				if (isInterrupt(err)) {
					return Promise.allSettled(promises)
						.then(() => new Promise(setImmediate)) // allow multiple jobs finishing a step on the same tick to continue in priority order
						.then(() => {
							// handle canceled task
							// update task record in storage, either w/ 'ready to continue' or 'sleeping until'
							// dispatch 'queue can handle next task' event
						})
				} else {
					// if not cancelled, this is an actual user-land error
				}
			}
		})

		return promise
	}
}