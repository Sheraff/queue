type Unit = 'Years' | 'Year' | 'Yrs' | 'Yr' | 'Y' | 'Weeks' | 'Week' | 'W' | 'Days' | 'Day' | 'D' | 'Hours' | 'Hour' | 'Hrs' | 'Hr' | 'H' | 'Minutes' | 'Minute' | 'Mins' | 'Min' | 'M' | 'Seconds' | 'Second' | 'Secs' | 'Sec' | 'S' | 'Milliseconds' | 'Millisecond' | 'Msecs' | 'Msec' | 'Ms'
type UnitAnyCase = Unit | Uppercase<Unit> | Lowercase<Unit>
export type Frequency =
	| `${number}`
	| `${number} per ${UnitAnyCase}`
	| `${number}/${UnitAnyCase}`
	| `${number} / ${UnitAnyCase}`
export type Duration =
	| `${number}`
	| `${number}${UnitAnyCase}`
	| `${number} ${UnitAnyCase}`


function reasonnable(str: string) {
	if (typeof str !== 'string' || str.length === 0 || str.length > 100) {
		throw new Error(
			'Value provided to must be a string with length between 1 and 99.',
		)
	}
}

const s = 1000
const m = s * 60
const h = m * 60
const d = h * 24
const w = d * 7
const y = d * 365.25

function toMs(type: string = 'ms'): number {
	const normalized = type.toLowerCase() as Lowercase<Unit>
	switch (normalized) {
		case 'years':
		case 'year':
		case 'yrs':
		case 'yr':
		case 'y':
			return y
		case 'weeks':
		case 'week':
		case 'w':
			return w
		case 'days':
		case 'day':
		case 'd':
			return d
		case 'hours':
		case 'hour':
		case 'hrs':
		case 'hr':
		case 'h':
			return h
		case 'minutes':
		case 'minute':
		case 'mins':
		case 'min':
		case 'm':
			return m
		case 'seconds':
		case 'second':
		case 'secs':
		case 'sec':
		case 's':
			return s
		case 'milliseconds':
		case 'millisecond':
		case 'msecs':
		case 'msec':
		case 'ms':
			return 1
		default:
			// This should never occur.
			throw new Error(
				`The unit ${type as string} was matched, but no matching case exists.`,
			)
	}
}

/** @returns a number of miliseconds */
export function parseDuration(str: Duration): number {
	reasonnable(str)
	const re = /^(?<value>-?(?:\d+)?\.?\d+) *(?<type>milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i
	const match = re.exec(str)
	const groups = match?.groups as { value: string; type?: string } | undefined
	if (!groups) {
		return NaN
	}
	const n = parseFloat(groups.value)
	return n * toMs(groups.type)
}

/** @returns a number of miliseconds between 2 events */
export function parsePeriod(str: Frequency): number {
	reasonnable(str)
	const re = /^(?<value>-?(?:\d+)?\.?\d+) *(?:per|\/) *(?<type>milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i
	const match = re.exec(str)
	const groups = match?.groups as { value: string; type?: string } | undefined
	if (!groups) {
		return NaN
	}
	const n = parseFloat(groups.value)
	return toMs(groups.type) / n
}