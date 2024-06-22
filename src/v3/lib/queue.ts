import { Pipe } from "./pipe"
import { fn, Job } from "./job"
import type { Storage } from "./storage"
import { execution, registration, type RegistrationContext } from "./context"

type SafeKeys<K extends string> = { [k in K]: { id: k } }

export class Queue<
	const Jobs extends { [key in string]: Job<key> } = { [key in string]: Job<key> },
	const Pipes extends { [key in string]: Pipe<key> } = { [key in string]: Pipe<key> },
> {
	public readonly id: string
	public readonly jobs: Jobs
	public readonly pipes?: Pipes
	public readonly storage: Storage
	constructor(opts: {
		id: string,
		jobs: Jobs & SafeKeys<keyof Jobs & string>,
		pipes?: Pipes & SafeKeys<keyof Pipes & string>,
		storage: Storage
	}) {
		this.id = opts.id
		this.jobs = Object.fromEntries(Object.entries(opts.jobs).map(([id, job]) => [
			id,
			new Proxy(job, {
				get: (target, prop) => {
					if (prop === 'dispatch') {
						return (...args: any[]) => registration.run(this.#getStore(), () => target.dispatch(...args))
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
						return (...args: any[]) => registration.run(this.#getStore(), () => target.dispatch(...args))
					}
					return Reflect.get(target, prop, target)
				}
			})
		])) as Pipes
		this.storage = opts.storage
		this.#start()
	}

	#getStore(): RegistrationContext {
		return {
			queue: this,
			checkRegistration: (instance) => {
				if (instance instanceof Job) return console.assert(instance.id in this.jobs, `Job ${instance.id} not registered in queue ${this.id}`)
				if (instance instanceof Pipe) return console.assert(this.pipes && instance.id in this.pipes, `Pipe ${instance.id} not registered in queue ${this.id}`)
				throw new Error('Unknown instance type')
			},
		}
	}

	#start() {
		// TODO: not the real implementation
		this.storage.startNextTask(this.id, (result) => {
			if (!result) return
			const [task, steps] = result
			const job = this.jobs[task.job]
			if (!job) throw new Error(`Job ${task.job} not registered in queue ${this.id}, but found for this queue in storage.`)
			execution.run({
				async run(options, fn) {
					const data = steps.find(step => step.step === options.id)
					if (data && data.status === 'success') return data.data && JSON.parse(data.data)
					const result = await execution.run(null, fn)
					return result
				},
				async sleep(ms) {
					return
				},
				async waitFor(instance, event, options) {
					return {} as any
				},
				async invoke(job, data) {
					return {} as any
				},
				dispatch(instance, data) {
					return
				}
			}, () =>
				registration.run(this.#getStore(), () =>
					job[fn](JSON.parse(task.input))
				)
			)
		})
	}

	async close() {
		await this.storage.close()
	}
}
