import { Pipe } from "./pipe"
import { exec, Job } from "./job"
import type { Storage } from "./storage"
import { registration, type RegistrationContext } from "./context"
import { hash } from "./utils"

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
					if (prop === 'dispatch') {
						return (...args: any[]) => registration.run(this.#getRegistrationContext(), () => target.dispatch(...args))
					}
					return Reflect.get(target, prop, target)
				}
			})
		])) as Jobs
		this.pipes = opts.pipes && Object.fromEntries(Object.entries(opts.pipes).map(([id, job]) => [
			id,
			new Proxy(job, {
				get: (target, prop) => {
					if (prop === 'dispatch') {
						return (...args: any[]) => registration.run(this.#getRegistrationContext(), () => target.dispatch(...args))
					}
					return Reflect.get(target, prop, target)
				}
			})
		])) as Pipes
		this.storage = opts.storage
		this.#start()
	}

	#getRegistrationContext(): RegistrationContext {
		return {
			queue: this,
			checkRegistration: (instance) => {
				if (instance instanceof Job) return console.assert(instance.id in this.jobs, `Job ${instance.id} not registered in queue ${this.id}`)
				if (instance instanceof Pipe) return console.assert(this.pipes && instance.id in this.pipes, `Pipe ${instance.id} not registered in queue ${this.id}`)
				throw new Error('Unknown instance type')
			},
			addTask: (job, data) => {
				const key = hash(data)
				this.storage.addTask({ queue: this.id, job: job.id, key, input: JSON.stringify(data) })
			}
		}
	}

	#running = new Set<Promise<any>>()
	#start(): 'none' | 'done' | Promise<'none' | 'done'> {
		const loopStatus = this.storage.startNextTask(this.id, (result) => {
			if (!result) return 'none'
			const [task, steps, hasNext] = result
			const job = this.jobs[task.job]
			if (!job) throw new Error(`Job ${task.job} not registered in queue ${this.id}, but found for this queue in storage.`)
			const promise = registration.run(this.#getRegistrationContext(), () => job[exec](this, task, steps))
			this.#running.add(promise)
			promise.finally(() => this.#running.delete(promise))
			if (hasNext) return this.#start()
			return 'done'
		})
		return loopStatus as 'none' | 'done' | Promise<'none' | 'done'>
	}

	#closed = false

	/** @public */
	async close() {
		if (this.#closed) {
			console.warn(`Queue ${this.id} already closed`)
			return
		}
		this.#closed = true
		await Promise.all(this.#running)
		await this.storage.close()
		for (const job of Object.values(this.jobs)) {
			job.close()
		}
	}
}
