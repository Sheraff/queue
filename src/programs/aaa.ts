import type { Ctx } from "../queue/queue.js"

type InitialData = {
	foo?: string
}

declare global {
	interface Program {
		aaa: {
			initial: InitialData
			result: {
				a: number
				start: number
				duration: number
				yolo: string
				c: number
			}
		}
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
	ctx.sleep(1)
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
		return {
			c: 3,
			duration,
		}
	})
}
