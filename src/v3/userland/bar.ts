import { Job } from "../lib"
import { foo, fooBarPipe } from "./foo"


export const bar = new Job({
	id: 'bar',
}, async (input: { name: string }) => {

	await Job.sleep(10)

	fooBarPipe.dispatch({ id: 1 })

	return 'hello'
})

foo.emitter.on('start', (data) => bar.dispatch({ name: String(data.id) }))