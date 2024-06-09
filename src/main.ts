import { aaa } from "./programs/aaa.js"
import { pokemon } from "./programs/pokemon.js"

import { handleNext, registerProgram, registerTask } from "./queue/queue.js"

// TODO: enforce that all programs are registered 
registerProgram({
	name: 'aaa',
	program: aaa,
})
registerProgram({
	name: 'pokemon',
	program: pokemon,
	options: {
		retry: 3,
		retryDelayMs: (attempt) => 2 ** attempt * 1000,
	}
})

// ids should be static for idempotency, but for now we'll just generate random ids
registerTask(crypto.randomUUID(), 'pokemon', { id: 2 })
registerTask(crypto.randomUUID(), 'pokemon', { id: 151 })

do {
	const hasMore = await handleNext()
	if (hasMore === 'done') break
	else if (hasMore === 'next') continue
	else if (hasMore === 'wait') await new Promise(resolve => setTimeout(resolve, 10))
	else throw new Error('Unknown handleNext result')
} while (true)


