import type { Event, SystemLog, Task } from 'queue'
import clsx from "clsx"
import { cleanEventName } from "./utils"

export function Events({
	events,
	job,
	hoveredEvent,
	setHoveredEvent,
	className
}: {
	events: (Event | SystemLog)[],
	job: Task,
	hoveredEvent: number[],
	setHoveredEvent: (event: number[]) => void,
	className?: string
}) {
	return (
		<div onMouseLeave={() => setHoveredEvent([])} className={className}>
			{events.map((event, i) => {
				const base = cleanEventName(event.key, job)
				const name = ('payload' in event)
					? `${base} - ${event.payload.event}`
					: base

				return (
					<div
						key={i}
						className={clsx("transition-all, px-2 py-1", hoveredEvent.includes(i) && 'bg-stone-200 dark:bg-stone-800')}
						onMouseEnter={() => setHoveredEvent([i])}
					>
						<span>{name}</span>
					</div>
				)
			})}
		</div>
	)
}