import type { Job } from "../lib"

export function invoke<J extends Job>(job: J, input: J["in"]): Promise<J['out']> {
	const done = new Promise<J['out']>((resolve, reject) => {
		job.emitter.on('settled', (input, output, error) => {
			if (error) return reject(error)
			resolve(output)
		})
	})
	job.dispatch(input)
	return done
}