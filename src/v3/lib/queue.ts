import type { Pipe } from "./pipe"
import type { Job } from "./job"
import type { Storage } from "./storage"

type SafeKeys<K extends string> = { [k in K]: { id: k } }

export class Queue<
	const Jobs extends { [key in string]: Job<key> },
	const Pipes extends { [key in string]: Pipe<key> },
> {
	public readonly jobs: Jobs
	public readonly pipes?: Pipes
	public readonly storage: Storage
	constructor(opts: {
		jobs: Jobs & SafeKeys<keyof Jobs & string>,
		pipes?: Pipes & SafeKeys<keyof Pipes & string>,
		storage: Storage
	}) {
		this.jobs = opts.jobs
		this.pipes = opts.pipes
		this.storage = opts.storage
	}

	async close() {
		await this.storage.close()
	}
}
