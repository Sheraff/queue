import { z } from "zod"
import { step, createProgram } from '../queue.js'
import { foo } from "./foo.js"

export const pokemon = createProgram({
	id: 'pokemon',
	output: z.object({ name: z.string() }),
	input: z.object({ id: z.number() }),
	triggers: { event: 'poke' }
}, async (input) => {

	const data = await step.run({ name: 'fetch', concurrency: { id: 'pokeapi' } }, async () => {
		const response = await fetch(`https://pokeapi.co/api/v2/pokemon/${input.id}`)
		return response.json() as Promise<{ name: string, order: number }>
	})

	step.dispatchProgram('foo', { fa: '2' })
	step.dispatchProgram(pokemon, { id: 23 })
	step.dispatchProgram(foo, { fa: '2' })
	// const eventData = await step.waitForEvent('foo-trigger')

	// step.dispatchEvent('poke', { fa: '1', id: 12 })

	// const dodo = await step.invokeProgram('foo', { fa: '1' })
	// const dudu = await step.invokeProgram(pokemon, { id: 23 })

	return { name: data.name }
})