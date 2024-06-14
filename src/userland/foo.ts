import { z } from "zod"
import { createProgram } from '../queue.js'

export const foo = createProgram({
	id: 'foo',
	input: z.object({ fa: z.string() }),
	output: z.object({ fi: z.string() }),
	triggers: { event: ['foo-trigger', 'poke'] }
}, async (input) => {
	return { fi: input.fa }
})