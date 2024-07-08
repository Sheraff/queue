import { useSyncExternalStore } from "react"


const values: Record<string, unknown> = {}
const subscribers = new Map<string, Set<() => void>>()

function subscribe(key: string, callback: () => void) {
	subscribers.get(key)!.add(callback)
	return () => subscribers.get(key)!.delete(callback)
}

function getSnapshot(key: string) {
	return values[key]
}

function setState(key: string, value: unknown) {
	values[key] = value
	for (const callback of subscribers.get(key) || []) {
		callback()
	}
	window.localStorage.setItem(key, JSON.stringify(value))
}

function init(key: string, defaultValue: unknown) {
	if (subscribers.has(key)) {
		throw new Error(`Key ${key} is already initialized. Key should be globally unique.`)
	}
	subscribers.set(key, new Set())
	const item = window.localStorage.getItem(key)
	if (item) {
		values[key] = JSON.parse(item)
	} else {
		values[key] = defaultValue
	}
}

export function createLocalStorageHook<T>(key: string, defaultValue: T) {
	init(key, defaultValue)
	const sub = (cb: () => void) => subscribe(key, cb)
	const snap = () => getSnapshot(key) as T
	const set = (value: T) => setState(key, value)
	const serve = () => defaultValue
	return function useStore() {
		const state = useSyncExternalStore<T>(sub, snap, serve)
		return [state, set] as const
	}
}