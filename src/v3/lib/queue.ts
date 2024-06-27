import { Pipe } from "./pipe"
import { exec, Job } from "./job"
import type { Storage } from "./storage"
import { registration, type RegistrationContext } from "./context"
import { hash, serializeError } from "./utils"

type SafeKeys<K extends string> = { [k in K]: { id: k } }

interface CronScheduler {
	(cron: string, cb: (date: Date | string) => void): () => void
}

export class Queue<
	const Jobs extends { [key in string]: Job<key> } = { [key in string]: Job<key> },
	const Pipes extends { [key in string]: Pipe<key, any> } = { [key in string]: Pipe<key> },
> {
	/** @public */
	public readonly id: string
	/** @public */
	public readonly jobs: Jobs
	/** @public */
	public readonly pipes: Pipes
	/** @public */
	public readonly storage: Storage
	/** @public */
	public readonly parallel: number

	constructor(opts: {
		id: string,
		jobs: Jobs & SafeKeys<keyof Jobs & string>,
		pipes?: Pipes & SafeKeys<keyof Pipes & string>,
		storage: Storage
		/** how many jobs can be started in parallel, defaults to `Infinity` */
		parallel?: number
		/** Any simple cron scheduler can be used, if not provided here, the `node-cron` package will be used instead. */
		cronScheduler?: CronScheduler
		// TODO: add logger options
		// TODO: add polling frequency (for cases where multiple queue workers are writing to the same storage)
	}) {
		this.id = opts.id
		this.parallel = Math.max(1, opts.parallel ?? Infinity)
		this.storage = opts.storage

		this.jobs = Object.fromEntries(Object.entries(opts.jobs).map(([id, job]) => [
			id,
			new Proxy(job, {
				get: (target, prop) => {
					if (prop === 'queue') return this
					const value = Reflect.get(job, prop, job)
					if (typeof value !== 'function') return value
					return (...args: any[]) => registration.run(this.#registrationContext, value.bind(target, ...args))
				}
			})
		])) as Jobs

		if (!opts.pipes) {
			this.pipes = {} as Pipes
		} else {
			this.pipes = Object.fromEntries(Object.entries(opts.pipes).map(([id, pipe]) => [
				id,
				new Proxy(pipe, {
					get: (target, prop) => {
						if (prop === 'queue') return this
						const value = Reflect.get(pipe, prop, pipe)
						if (typeof value !== 'function') return value
						return (...args: any[]) => registration.run(this.#registrationContext, value.bind(target, ...args))
					}
				})
			])) as Pipes
		}

		for (const job of Object.values(this.jobs)) {
			job.start()
		}
		this.#loop()
		this.#cron(opts.cronScheduler)
	}

	#registrationContext: RegistrationContext = {
		queue: this,
		checkRegistration: (instance) => {
			if (instance instanceof Job) return console.assert(instance.id in this.jobs, `Job ${instance.id} not registered in queue ${this.id}`)
			if (instance instanceof Pipe) return console.assert(this.pipes && instance.id in this.pipes, `Pipe ${instance.id} not registered in queue ${this.id}`)
			throw new Error('Unknown instance type')
		},
		addTask: (job, data, key, parent, priority, debounce, throttle, rateLimit, cb) => {
			return this.storage.addTask({
				queue: this.id,
				job: job.id,
				key,
				input: JSON.stringify(data),
				parent_id: parent ?? null,
				priority,
				debounce: debounce ?? null,
				throttle: throttle ?? null,
				rateLimit: rateLimit ?? null,
			}, (inserted, cancelled) => {
				this.#loop()
				return cb(inserted, cancelled)
			})
		},
		resolveTask: (task, status, data, cb) => {
			const output = status === 'failed' ? serializeError(data) : JSON.stringify(data)
			return this.storage.resolveTask(task, status, output, () => {
				this.#loop()
				return cb()
			})
		},
		requeueTask: (task, cb) => {
			return this.storage.requeueTask(task, cb)
		},
		recordStep: (task, step, cb) => {
			return this.storage.recordStep(task, step, cb)
		},
		recordEvent: (key, input, data) => {
			return this.storage.recordEvent(this.id, key, input, data, () => this.#loop())
		},
		triggerJobsFromPipe: (pipe, input) => {
			for (const job of Object.values(this.jobs)) {
				if (!job.triggers) continue
				for (const trigger of job.triggers) {
					if (trigger instanceof Pipe) {
						if (trigger !== pipe) continue
						job.dispatch(input)
					} else {
						const [p, transform] = trigger
						if (p !== pipe) continue
						job.dispatch(transform(input))
					}
				}
			}
		}
	}

	#running = new Set<Promise<any>>()
	#willRun = false
	#sleepTimeout: NodeJS.Timeout | null = null

	#drain(): void | Promise<void> {
		return this.storage.startNextTask(this.id, (result) => {
			if (!result) return

			const [task, steps, hasNext] = result
			const job = this.jobs[task.job]
			if (!job) throw new Error(`Job ${task.job} not registered in queue ${this.id}, but found for this queue in storage.`)

			const promise = registration.run(this.#registrationContext, () => job[exec](this.#registrationContext, task, steps))

			this.#running.add(promise)
			promise.finally(() => {
				this.#running.delete(promise)
				this.#loop()
			})

			if (hasNext && this.#running.size < this.parallel) return this.#drain()
		}) as void | Promise<void>
	}

	// TODO: should this be public? Might be useful for cases where multiple queue workers are writing to the same storage, and we don't want to poll and would rather have a manual trigger.
	#loop() {
		if (this.#willRun || this.#closed) return
		if (this.#running.size >= this.parallel) return
		this.#willRun = true
		if (this.#sleepTimeout) {
			clearTimeout(this.#sleepTimeout)
			this.#sleepTimeout = null
		}
		setImmediate(async () => {
			if (this.#closed) return
			await this.#drain()
			this.#willRun = false
			setImmediate(() => {
				if (this.#willRun || this.#closed) return
				this.storage.nextFutureTask(this.id, (result) => {
					if (this.#willRun || this.#closed) return
					if (result.ms === null) return // program will exit unless something else (outside of this queue) is keeping it open
					this.#sleepTimeout = setTimeout(
						() => this.#loop(),
						Math.max(0, result.ms)
					)
				})
			})
		})
	}

	#cronjobs: Array<() => void> = []
	async #cron(externalImplementation?: CronScheduler) {
		const schedule: CronScheduler = externalImplementation
			|| await import('node-cron')
				.catch((error: Error) => {
					throw new ReferenceError('To use `cron` triggers on jobs, install "node-cron" or provide a `cronScheduler` to the queue.', { cause: error })
				})
				.then(({ schedule }) =>
					(str: string, cb: (date: Date | string) => void) => {
						const emitter = schedule(str, cb)
						return emitter.stop.bind(emitter)
					}
				)
		for (const job of Object.values(this.jobs)) {
			if (!job.cron) continue
			const schedules = typeof job.cron === 'string' ? [job.cron] : job.cron
			let willExec = false
			for (const string of schedules) {
				this.#cronjobs.push(schedule(string, (date) => {
					if (typeof date === 'string') return
					// in case multiple cron schedules happen to overlap in the same second, we only want to run the job once
					// for example: every hour '0 * * * *' and every minute '* * * * *' would both run at the same time at the start of the hour
					if (willExec) return
					willExec = true
					setImmediate(() => {
						willExec = false
						job.dispatch({ date: date.toISOString() })
					})
				}))
			}
		}
	}

	#closed = false

	/**
	 * @public
	 * 
	 * Finalize everything in the queue safely, waiting for the currently running
	 * jobs to reach a safe state before closing the queue.
	 * 
	 * ⚠️ If the `storage.db` was passed as an argument,
	 * it will not be closed as it is considered to be managed externally.
	 */
	async close() {
		if (this.#closed) {
			console.warn(`Queue ${this.id} already closed`)
			return
		}
		this.#closed = true

		// kill timeout, we're exiting
		if (this.#sleepTimeout) {
			clearTimeout(this.#sleepTimeout)
			this.#sleepTimeout = null
		}

		// stop cron jobs
		for (const stop of this.#cronjobs) {
			stop()
		}

		// let all running jobs finish
		while (this.#running.size) {
			await Promise.all(this.#running)
		}

		// close all jobs
		for (const job of Object.values(this.jobs)) {
			job.close()
		}

		// close database
		await this.storage.close()
	}
}
