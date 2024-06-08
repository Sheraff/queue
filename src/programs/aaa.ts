import type { Ctx } from "../main"

export function aaa(ctx: Ctx) {
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
