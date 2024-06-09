import type { Ctx, ProgramEntry } from "../queue/queue.js"

type InitialData = {
	mimi: string
}

declare global {
	interface Registry {
		notask: ProgramEntry<InitialData>
	}
}


export function notask(ctx: Ctx<InitialData>) {

}
