import { defineProgram, type ProgramEntry } from "../queue/queue.js"

declare global {
	interface Registry {
		aaa: ProgramEntry<{
			foo?: string
			registered_on: number
		}, {
			start: number
			a: number
			yolo: string
			c: number
			duration: number
			since_registered: number | undefined
		}>
	}
}

export const aaa = defineProgram('aaa', {
	concurrency: 1,
	delayBetweenMs: 500,
}, (ctx) => ctx
	.step(() => {
		// do something
		return {
			start: Date.now(),
			a: 2,
		}
	})
	.step((data) => {
		data.a
		//   ^?
	})
	.sleep(500)
	.step(() => {
		return {
			yolo: "hello",
			a: 2
		} as const
	})
	.step((data) => {
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
)

