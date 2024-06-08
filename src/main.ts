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
	if (!hasMore) break
} while (true)


