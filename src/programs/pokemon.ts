import { registerTask, type Ctx, type Data } from "../queue/queue.js"

type InitialData = {
	id: number
}

declare global {
	interface Program {
		pokemon: {
			initial: InitialData
			result: InitialData & {
				next_id: number
				species_url: string
				evolution_chain_url: string
				chain: string[]
				next?: Program['pokemon']['result']
			}
		}
	}
}

/**
 * a queued job that fetches a pokemon by id, and then fetches all the evolutions of that pokemon
 */
export function pokemon(ctx: Ctx<InitialData>) {
	ctx.step(async (data) => {
		const res = await fetch(`https://pokeapi.co/api/v2/pokemon/${data.id}`)
		const pokemon = await res.json() as any
		return {
			species_url: pokemon.species.url,
		}
	})
	ctx.step(async (data) => {
		const res = await fetch(data.species_url)
		const species = await res.json() as any
		return {
			evolution_chain_url: species.evolution_chain.url,
		}
	})
	ctx.registerTask('pokemon', { id: ctx.data.id + 1 }, 'next', (data) => data.id === 151)
	ctx.step(async (data) => {
		const res = await fetch(data.evolution_chain_url)
		const evolutionChain = await res.json() as any
		let pokemon = evolutionChain.chain
		const chain = []
		while (pokemon) {
			chain.push(pokemon.species.name)
			pokemon = pokemon.evolves_to[0]
		}
		if (chain.length > 2) {
			registerTask(crypto.randomUUID(), 'aaa', { foo: `from pokemon ${data.id}` })
		}
		return {
			chain,
			next_id: data.next?.id
		}
	})
}
