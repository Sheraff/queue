import type { Job } from "../lib"

export function invoke<J extends Job>(job: J, input: J["in"]): Promise<J['out']> {
	const done = new Promise<J['out']>((resolve, reject) => {
		job.emitter.on('settled', ({ result, error }, meta) => {
			if (meta.queue !== job.queue.id) return
			if (key !== meta.key) return
			if (error) return reject(error)
			resolve(result)
		})
	})
	const key = job.dispatch(input)
	return done
}