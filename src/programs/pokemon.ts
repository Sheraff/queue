import { defineProgram, registerTask, type ProgramEntry } from "../queue/queue.js"


declare global {
	interface Registry {
		pokemon: ProgramEntry<{
			id: number
		}, {
			species_url: string
			awaited?: Registry['aaa']['result']
			evolution_chain_url?: string
			chain?: string[]
			next?: Registry['pokemon']['result']
			next_id?: number
		}>
	}
}

/**
 * a queued job that fetches a pokemon by id, and then fetches all the evolutions of that pokemon
 */
export const pokemon = defineProgram('pokemon', {
	retry: 1,
	retryDelayMs: (attempt) => 2 ** attempt * 1000,
	priority: (data) => data.id === 151 ? 2 : 1,
}, (ctx) => ctx
	.step(async (data) => {
		const res = await fetch(`https://pokeapi.co/api/v2/pokemon/${data.id}`)
		const pokemon = await res.json() as any
		setTimeout(() => {
			registerTask(crypto.randomUUID(), 'aaa', { foo: `wait for target ${data.id}` })
		}, 500)
		return {
			species_url: pokemon.species.url as string,
		}
	})
	.waitForTask('aaa', 'awaited', ['foo', `wait for target ${ctx.data.id}`])
	.done((data) => data.id === 3)
	.step(async (data) => {
		const res = await fetch(data.species_url)
		const species = await res.json() as any
		if (data.id === 152) throw new Error('this is a test error')
		return {
			evolution_chain_url: species.evolution_chain.url as string,
		}
	})
	.registerTask('pokemon', { id: ctx.data.id + 1 }, 'next', (data) => data.id === 151 || data.id === 2)
	.step(async (data) => {
		const res = await fetch(data.evolution_chain_url)
		const evolutionChain = await res.json() as any
		let pokemon = evolutionChain.chain
		const chain: string[] = []
		while (pokemon) {
			chain.push(pokemon.species.name)
			pokemon = pokemon.evolves_to[0]
		}
		return {
			chain,
			next_id: data.next?.id,
			awaited_result: data.awaited?.foo,
		}
	})
)

