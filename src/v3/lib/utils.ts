import { createHash } from "crypto"
import type { Data } from "./types"

function serialize(obj: Data): string {
	if (obj === undefined) return 'undefined'
	if (!obj || typeof obj !== 'object') return JSON.stringify(obj)
	if (Array.isArray(obj)) return `[${obj.map(serialize).join(',')}]`
	const keys = Object.keys(obj).sort()
	return `{${keys.map((key) => `"${key}":${serialize(obj[key])}`).join(',')}}`
}

function md5(input: string): string {
	return createHash('md5').update(Buffer.from(input)).digest('hex')
}

export function hash(input: Data) {
	const string = serialize(input)
	if (string.length < 40) return string
	return md5(string)
}

export function isPromise(obj: unknown): obj is Promise<any> {
	return !!obj && typeof obj === 'object' && 'then' in obj && typeof obj.then === 'function'
}

const interruptToken = Symbol('interrupt')
export function interrupt() {
	throw interruptToken
}
export function isInterrupt(err: any): err is typeof interruptToken {
	return err === interruptToken
}

export class NonRecoverableError extends Error {
	constructor(message?: string, options?: ErrorOptions) {
		super(message, options)
		this.name = 'NonRecoverableError'
	}
}