import { Queue } from '../queue.js'
import { foo } from "./foo.js"
import { pokemon } from "./pokemon.js"

// const registry = registerPrograms({
// 	pokemon,
// 	foo,
// })

// declare global {
// 	interface Registry2 {
// 		registry: typeof queue.registry
// 	}
// }

export const queue = new Queue({
	pokemon,
	foo,
}, {
	dbName: 'woop.db'
})

queue.registry.pokemon.invoke({ id: 25 }).catch(() => { })
// pokemon.invoke({ id: 25 }).catch(() => { })