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
			a: 2,
		}
	})
	ctx.step((data) => {
		data.a
		//   ^?
	})
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
		return {
			c: 3
		}
	})
}
