import { createHash } from "crypto"
import type { Data } from "./types"

// https://github.com/erdtman/canonicalize/blob/master/lib/canonicalize.js
function serialize(object: Data): string {
	if (typeof object === 'number' && isNaN(object)) {
		throw new Error('NaN is not allowed')
	}

	if (typeof object === 'number' && !isFinite(object)) {
		throw new Error('Infinity is not allowed')
	}

	if (object === null || typeof object !== 'object') {
		return JSON.stringify(object)
	}

	if (Array.isArray(object)) {
		const values = object.reduce((t, cv, ci) => {
			const comma = ci === 0 ? '' : ','
			const value = cv === undefined || typeof cv === 'symbol' ? null : cv
			return `${t}${comma}${serialize(value)}`
		}, '')
		return `[${values}]`
	}

	const values = Object.keys(object).sort().reduce((t, cv) => {
		if (object[cv] === undefined ||
			typeof object[cv] === 'symbol') {
			return t
		}
		const comma = t.length === 0 ? '' : ','
		return `${t}${comma}${serialize(cv)}:${serialize(object[cv])}`
	}, '')
	return `{${values}}`
};

// TODO: use `serialize-error` package instead
export function serializeError(error: unknown): string {
	const e = error instanceof Error
		? error
		: new Error(JSON.stringify(error))
	const cause = e.cause
	if (cause instanceof Error) {
		return JSON.stringify({
			message: cause.message,
			stack: cause.stack,
			cause: cause.cause ? serializeError(cause) : undefined,

		})
	}
	return JSON.stringify({
		message: e.message,
		stack: e.stack,
	})
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

export const interrupt = Symbol('interrupt')
export function isInterrupt(err: any): err is typeof interrupt {
	return err === interrupt
}

export class NonRecoverableError extends Error {
	override name = 'NonRecoverableError'
}

export function hydrateError(serialized: string): Error {
	const obj = JSON.parse(serialized)
	const error = new Error(obj.message)
	error.stack = obj.stack
	if (obj.cause) error.cause = hydrateError(obj.cause)
	return error

}