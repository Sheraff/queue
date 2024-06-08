import { aaa } from "./programs/aaa.js"
import { pokemon } from "./programs/pokemon.js"

import { handleNext, registerProgram, registerTask } from "./queue/queue.js"

registerProgram('aaa', aaa)
registerProgram('pokemon', pokemon)

// ids should be static for idempotency, but for now we'll just generate random ids
registerTask(crypto.randomUUID(), 'aaa', {})
registerTask(crypto.randomUUID(), 'pokemon', { id: 2 })
registerTask(crypto.randomUUID(), 'aaa', {})
registerTask(crypto.randomUUID(), 'aaa', {})
registerTask(crypto.randomUUID(), 'pokemon', { id: 151 })

do {
	const hasMore = await handleNext()
	if (hasMore === 'done') break
	else if (hasMore === 'next') continue
	else if (hasMore === 'wait') await new Promise(resolve => setTimeout(resolve))
	else throw new Error('Unknown handleNext result')
} while (true)


