import type { Ctx } from "../queue/queue.js"

type InitialData = {
	mimi: string
}

declare global {
	interface Program {
		notask: {
			initial: InitialData
			result: InitialData
		}
	}
}


export function notask(ctx: Ctx<InitialData>) {

}
