import type { Event, Task } from 'queue'
import clsx from "clsx"
import { cleanEventName } from "./utils"

export function Events({
	events,
	job,
	hoveredEvent,
	setHoveredEvent,
	className
}: {
	events: Event[],
	job: Task,
	hoveredEvent: number[],
	setHoveredEvent: (event: number[]) => void,
	className?: string
}) {
	return (
		<div onMouseLeave={() => setHoveredEvent([])} className={className}>
			{events.map((event, i) => {
				const name = cleanEventName(event.key, job)
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