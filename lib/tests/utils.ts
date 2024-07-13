import type { Job } from "../src"

export function invoke<J extends Job>(job: J, input: J["in"]): Promise<J['out']> {
	const done = new Promise<J['out']>((resolve, reject) => {
		type OnSettled = Parameters<typeof job.emitter.on<'settled'>>[1]
		const onSettled: OnSettled = ({ result, error, reason }, meta) => {
			if (meta.queue !== job.queue.id) return
			if (key !== meta.key) return
			job.emitter.off('settled', onSettled)
			if (error) return reject(error)
			if (reason) return reject(reason)
			resolve(result)
		}
		job.emitter.on('settled', onSettled)
	})
	const key = job.dispatch(input)
	return done
}