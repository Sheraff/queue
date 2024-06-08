import type { Ctx, Data } from "../main"

/**
 * a queued job that fetches a pokemon by id, and then fetches all the evolutions of that pokemon
 */
export function pokemon(ctx: Ctx<{ id: number }>) {
	ctx.step(async (data) => {
		const res = await fetch(`https://pokeapi.co/api/v2/pokemon/${data.id}`)
		const pokemon = await res.json() as Data
		return {
			pokemon
		}
	})
	ctx.step(async (data) => {
		const res = await fetch((data.pokemon as any).species.url)
		const species = await res.json() as any
		return {
			species
		}
	})
	ctx.step(async (data) => {
		const res = await fetch(data.species.evolution_chain.url)
		const evolutionChain = await res.json() as any
		return {
			evolutionChain
		}
	})
}
