import type { Ctx, ProgramEntry } from "../queue/queue.js"

type InitialData = {
	foo?: string
	registered_on?: number
}

declare global {
	interface Registry {
		aaa: ProgramEntry<InitialData, {
			start: number
			a: number
			yolo: string
			c: number
			duration: number
			since_registered?: number
		}>
	}
}


export function aaa(ctx: Ctx<InitialData>) {
	ctx.step(() => {
		// do something
		return {
			start: Date.now(),
			a: 2,
		}
	})
	ctx.step((data) => {
		data.a
		//   ^?
	})
	// ctx.sleep(1000)
	ctx.step(() => {
		return {
			yolo: "hello",
			a: 2
		} as const
	})
	ctx.step((data) => {
		data.yolo
		//   ^?
		data.a
		//   ^?
		const end = Date.now()
		const duration = end - data.start
		const since_registered = data.registered_on && end - data.registered_on
		return {
			c: 3,
			duration,
			since_registered,
		}
	})
}
