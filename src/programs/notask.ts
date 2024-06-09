import { defineProgram, type ProgramEntry } from "../queue/queue.js"

declare global {
	interface Registry {
		notask: ProgramEntry<{ mimi: string }>
	}
}

export const notask = defineProgram('notask', {}, (ctx) => ctx)

