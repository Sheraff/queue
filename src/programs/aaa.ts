import { defineProgram, type ProgramEntry } from "../queue/queue.js"

declare global {
	interface Registry {
		aaa: ProgramEntry<{
			foo?: string
		}, {
			a: number
			yolo: string
			c: number
			since_started: number
			since_registered: number
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
	.step((data, task) => {
		data.yolo
		//   ^?
		data.a
		//   ^?
		const end = Date.now()
		const start = task.started_at! * 1000
		const registered = task.created_at! * 1000
		return {
			c: 3,
			since_registered: end - registered,
			since_started: end - start,
		}
	})
)

