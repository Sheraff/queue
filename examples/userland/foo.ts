import { Job, Pipe } from "../../lib/src"

export const fooBarPipe = new Pipe({
	id: 'fooBarPipe',
	in: {} as { id: number },
})

export const otherPipe = new Pipe({
	id: 'otherPipe',
	in: {} as { id: string },
})

export const foo = new Job({
	id: 'foo',
	triggers: [otherPipe.into(({ id }) => ({ id: Number(id) }))],
}, async (input: { id: number }) => {

	const a = await Job.run('a', async () => {
		await new Promise((resolve) => setTimeout(resolve, 10))
		return 'abc'
	})

	const data = await Job.waitFor(fooBarPipe)

	await Job.sleep("1s")

	return a
})