import { aaa } from "./programs/aaa.js"
import { pokemon } from "./programs/pokemon.js"
import { notask } from "./programs/notask.js"

import { handleNext, registerPrograms, registerTask } from "./queue/queue.js"


registerPrograms({
	...aaa,
	...pokemon,
	...notask,
})

// ids should be static for idempotency, but for now we'll just generate random ids
registerTask(crypto.randomUUID(), 'aaa', {})
registerTask(crypto.randomUUID(), 'aaa', {})
registerTask(crypto.randomUUID(), 'pokemon', { id: 2 })
registerTask(crypto.randomUUID(), 'pokemon', { id: 151 })
registerTask(crypto.randomUUID(), 'notask', { mimi: 'momo' })
registerTask(crypto.randomUUID(), 'notask', { mimi: 'mumu' })

do {
	const hasMore = await handleNext()
	if (hasMore === 'done') break
	if (hasMore === 'next') continue
	if (hasMore === 'wait') await new Promise(setImmediate)
	else throw new Error('Unknown handleNext result')
} while (true)