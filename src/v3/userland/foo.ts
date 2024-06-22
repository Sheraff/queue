import { Job, Pipe } from "../lib"

export const fooBarPipe = new Pipe({
	id: 'fooBarPipe',
	in: {} as { id: number },
})

export const foo = new Job({
	id: 'foo',
}, async (input: { id: number }) => {

	const a = await Job.run('a', async () => {
		await new Promise((resolve) => setTimeout(resolve, 10))
		return 'abc'
	})

	const data = await fooBarPipe.waitFor()

	await Job.sleep(10)

	return a
})