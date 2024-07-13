import { once } from "node:events"
import { createReadStream } from "node:fs"
import { createInterface } from "node:readline"
import type { Event } from './storage'
import pino from "pino"
import type { Data } from "./types"

/** @package */
export const system = Symbol('system')

type SystemPayload =
	| { event: 'success', data: Data, runs: number }
	| { event: 'error', error: string, runs: number }
	| { event: 'run', runs: number }

export interface Logger {
	/** create a new logger with some pre-filled data */
	child(payload: object): Logger
	/** internal log type, highest level, specific shape */
	[system](payload: SystemPayload): void
	info(message: string): void
	info(payload: object): void
	warn(message: string): void
	warn(payload: object): void
	error(message: string): void
	error(payload: object): void
}

export interface LogReader {
	get(
		query: {
			queue: string,
			job: string,
			input: string
		},
		onLine: (line: Log) => void
	): Promise<void>
}

export class PinoLogger implements Logger {
	#pino: pino.Logger<"system">

	constructor(opts?: {
		/** @public */
		dest?: string
		/** @package */
		from?: pino.Logger<"system">
	}) {
		this.#pino = opts?.from ?? pino({
			customLevels: {
				system: Number.MAX_SAFE_INTEGER
			},
			nestedKey: 'payload',
			messageKey: 'message',
			errorKey: 'error',
		}, pino.destination({
			dest: opts?.dest ?? './queue.log.jsonl',
			sync: false
		}))

		this[system] = this.#pino.system.bind(this.#pino)
		this.info = this.#pino.info.bind(this.#pino)
		this.warn = this.#pino.warn.bind(this.#pino)
		this.error = this.#pino.error.bind(this.#pino)
	}

	[system]: Logger[typeof system]
	info: Logger['info']
	warn: Logger['warn']
	error: Logger['error']

	child(payload: object) {
		const child = this.#pino.child(payload)
		return new PinoLogger({ from: child })
	}
}

type BaseLog = Pick<Event, 'queue' | 'created_at' | 'key' | 'input'> & {
	fromLogger: true,
}

export type UserLog = BaseLog & {
	system: false,
	payload: string | object
}

export type SystemLog = BaseLog & {
	system: true,
	payload: SystemPayload
}

export type Log = UserLog | SystemLog

export class PinoReader implements LogReader {
	#dest: string

	constructor(opts?: {
		dest?: string
	}) {
		this.#dest = opts?.dest ?? './queue.log.jsonl'
	}


	async get(
		query: {
			queue: string,
			job: string,
			input: string
		},
		onLine: (line: Log) => void
	) {
		const rl = createInterface({
			input: createReadStream(this.#dest),
			crlfDelay: Infinity,
		})

		const innerOnLine = lineFilter(query, onLine)

		rl.on('line', innerOnLine)
		await once(rl, 'close')
		rl.off('line', innerOnLine)
	}
}

function lineFilter(query: { queue: string, job: string, input: string }, onLine: (line: Log) => void) {
	const filter = `"queue":"${query.queue}","job":"${query.job}","input":${JSON.stringify(query.input)},`

	return (line: string) => {
		const index = line.indexOf(filter)
		if (index === -1) return

		const raw = JSON.parse(line) as {
			time: number
			queue: string
			job: string
			input: string
			key: string
			runs: number
			payload?: object
			message?: string
			level: number
		}

		onLine({
			queue: raw.queue,
			key: raw.key,
			created_at: raw.time / 1000,
			input: raw.input,
			fromLogger: true,
			system: (raw.level === Number.MAX_SAFE_INTEGER) as never,
			payload: raw.payload ?? raw.message ?? '',
		})
	}
}

export class InMemoryLogger implements Logger, LogReader {
	#acc: object
	#logs: string[]

	constructor(acc: object = {}, logs: string[] = []) {
		this.#acc = acc
		this.#logs = logs
	}

	#push(log: object) {
		this.#logs.push(JSON.stringify(log))
	}

	child(acc: object) {
		return new InMemoryLogger({ ...this.#acc, ...acc }, this.#logs)
	}


	[system](payload: SystemPayload) {
		this.#push({
			level: Number.MAX_SAFE_INTEGER,
			time: Date.now(),
			...this.#acc,
			payload,
		})
	}

	#base(payload: string | object) {
		if (typeof payload === 'string') {
			this.#push({
				level: 30,
				time: Date.now(),
				...this.#acc,
				message: payload,
			})
		} else {
			this.#push({
				level: 30,
				time: Date.now(),
				...this.#acc,
				payload,
			})
		}
	}

	info(payload: object | string) {
		this.#base(payload)
		console.log(payload)
	}
	warn(payload: object | string) {
		this.#base(payload)
		console.warn(payload)
	}
	error(payload: object | string) {
		this.#base(payload)
		console.error(payload)
	}

	async get(
		query: {
			queue: string,
			job: string,
			input: string
		},
		onLine: (line: Log) => void
	) {
		const innerOnLine = lineFilter(query, onLine)

		for (const line of this.#logs) {
			innerOnLine(line)
		}
	}
}