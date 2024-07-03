import { Job } from "../../lib/src"
import { foo, fooBarPipe } from "./foo"


export const bar = new Job({
	id: 'bar',
}, async (input: { name: string }) => {

	await Job.sleep("10 ms")

	fooBarPipe.dispatch({ id: 1 })

	const fifoo = await Job.invoke(foo, { id: 1 })

	return 'hello'
})

foo.emitter.on('start', ({ input }) => bar.dispatch({ name: String(input.id) }))