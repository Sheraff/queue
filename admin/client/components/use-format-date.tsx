import { useMemo } from "react"
import { useNow } from "client/now"
import { useTimeDisplay } from "client/components/time-display-provider"

function secondsToHumanReadable(seconds: number): [number, Intl.RelativeTimeFormatUnit] {
	let unit: Intl.RelativeTimeFormatUnit = 'second'
	if (seconds > 60) {
		seconds /= 60
		unit = 'minute'
		if (seconds > 60) {
			seconds /= 60
			unit = 'hour'
			if (seconds > 24) {
				seconds /= 24
				unit = 'day'
				if (seconds > 364) {
					seconds /= 365
					unit = 'year'
				} else if (seconds > 30) {
					seconds /= 30
					unit = 'month'
				} else if (seconds > 7) {
					seconds /= 7
					unit = 'week'
				}
			}
		}
	}
	return [seconds, unit]
}

export function useFormatDate(value: number) {
	const now = useNow()
	const [variant] = useTimeDisplay()
	return useMemo(() => {
		if (variant === 'absolute') {
			const date = new Date(value * 1000)
			const formatter = new Intl.DateTimeFormat("en", {
				year: "numeric",
				month: "short",
				day: "2-digit",
				hour: "2-digit",
				minute: "2-digit",
				second: "2-digit",
			})
			return formatter.format(date)
		}
		if (variant === 'relative') {
			const seconds = now - value
			const [diff, unit] = secondsToHumanReadable(seconds)
			const relative = new Intl.RelativeTimeFormat('en', { style: 'long', numeric: 'auto' })
			return relative.format(-Math.round(diff), unit)
		}
		throw new Error(`Unknown time variant: ${variant}`)
	}, [value, now, variant])
}