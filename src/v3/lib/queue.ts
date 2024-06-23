import { Pipe } from "./pipe"
import { exec, Job } from "./job"
import type { Storage } from "./storage"
import { registration, type RegistrationContext } from "./context"
import { hash, serializeError } from "./utils"

type SafeKeys<K extends string> = { [k in K]: { id: k } }

export class Queue<
	const Jobs extends { [key in string]: Job<key> } = { [key in string]: Job<key> },
	const Pipes extends { [key in string]: Pipe<key> } = { [key in string]: Pipe<key> },
> {
	/** @public */
	public readonly id: string
	/** @public */
	public readonly jobs: Jobs
	/** @public */
	public readonly pipes?: Pipes
	/** @public */
	public readonly storage: Storage

	constructor(opts: {
		id: string,
		jobs: Jobs & SafeKeys<keyof Jobs & string>,
		pipes?: Pipes & SafeKeys<keyof Pipes & string>,
		storage: Storage
		// TODO: add logger options
		// TODO: add cron implementation injection
	}) {
		this.id = opts.id
		this.jobs = Object.fromEntries(Object.entries(opts.jobs).map(([id, job]) => [
			id,
			new Proxy(job, {
				get: (target, prop) => {
					const value = Reflect.get(job, prop, job)
					if (typeof value !== 'function') return value
					return (...args: any[]) => registration.run(this.#registrationContext, () => value.bind(target)(...args))
				}
			})
		])) as Jobs
		this.pipes = opts.pipes && Object.fromEntries(Object.entries(opts.pipes).map(([id, pipe]) => [
			id,
			new Proxy(pipe, {
				get: (target, prop) => {
					const value = Reflect.get(pipe, prop, pipe)
					if (typeof value !== 'function') return value
					return (...args: any[]) => registration.run(this.#registrationContext, () => value.bind(target)(...args))
				}
			})
		])) as Pipes
		this.storage = opts.storage
		this.#start()
	}

	#registrationContext: RegistrationContext = {
		queue: this,
		checkRegistration: (instance) => {
			if (instance instanceof Job) return console.assert(instance.id in this.jobs, `Job ${instance.id} not registered in queue ${this.id}`)
			if (instance instanceof Pipe) return console.assert(this.pipes && instance.id in this.pipes, `Pipe ${instance.id} not registered in queue ${this.id}`)
			throw new Error('Unknown instance type')
		},
		addTask: (job, data) => {
			const key = hash(data)
			this.storage.addTask({ queue: this.id, job: job.id, key, input: JSON.stringify(data) }, () => this.#start())
		},
		resolveTask: (task, status, data, cb) => {
			const output = status === 'failed' ? serializeError(data) : JSON.stringify(data)
			return this.storage.resolveTask(task, status, output, cb)
		},
		requeueTask: (task, cb) => {
			return this.storage.requeueTask(task, cb)
		},
		recordStep: (job, task, memo, step, cb) => {
			return this.storage.recordStep(job.id, task, memo, step, cb)
		},
	}

	#running = new Set<Promise<any>>()
	#willRun = false
	#loopExec(): 'none' | 'done' | Promise<'none' | 'done'> {
		return this.storage.startNextTask(this.id, (result) => {
			if (!result) {
				this.#willRun = false
				return 'none'
			}
			const [task, steps, hasNext] = result
			const job = this.jobs[task.job]
			if (!job) throw new Error(`Job ${task.job} not registered in queue ${this.id}, but found for this queue in storage.`)

			const promise = registration.run(this.#registrationContext, () => job[exec](this.#registrationContext, task, steps))

			this.#running.add(promise)
			promise.finally(() => {
				this.#running.delete(promise)
				this.#start()
			})

			if (hasNext) return this.#loopExec()

			this.#willRun = false
			return 'done'
		}) as 'none' | 'done' | Promise<'none' | 'done'>
	}

	#start() {
		if (this.#willRun) return
		this.#willRun = true
		if (this.#closed) return
		setImmediate(async () => {
			if (this.#closed) return
			const status = await this.#loopExec()
			if (status === 'none') {
				// TODO: query storage to know "if nothing changes" when to run again
			}
		})
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
