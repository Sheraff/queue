import type { Queue } from "./queue"


export function exhaustQueue(queue: Queue<any>) {
	return new Promise<void>((resolve) => {
		let timeoutId = setTimeout(resolve, 20)
		listenAll(queue, () => {
			clearTimeout(timeoutId)
			timeoutId = setTimeout(resolve, 20)
		})
	})
}

export function listenAll(queue: Queue<any>, cb: (event: string) => void) {
	const oldEmit = queue.emitter.emit
	// @ts-ignore
	queue.emitter.emit = function () { cb(arguments[0]); oldEmit.apply(queue.emitter, arguments) }
}