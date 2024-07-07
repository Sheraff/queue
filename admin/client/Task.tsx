import { useQuery } from "@tanstack/react-query"
import { Fragment, useRef, useState } from "react"
import type { Step, Event, Task } from 'queue'
import { Button } from "client/components/ui/button"
import { Code } from "client/components/syntax-highlighter"
import clsx from "clsx"

type Data = { steps: Step[], events: Event[], date: number }

const refetch: Record<string, number> = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

const cleanEventName = (name: string, job: { job: string }) => name.replace(new RegExp(`^job\\/${job.job}\\/`), '')
	.replace(new RegExp(`^step\\/${job.job}\\/`), '')

export function TaskPage({ id, job, setJob }: { id: number, job: Task, setJob: (job: number) => void }) {

	const { data, isFetching } = useQuery({
		queryKey: ['tasks', id],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${id}`)
			const json = await res.json()
			return json as Data
		},
		refetchInterval: refetch[job.status] ?? false
	})

	const [hoveredEvent, setHoveredEvent] = useState<number[]>([])

	return (
		<div className="flex-1">
			<h2 className="text-xl">Task {job.input}{isFetching && ' - fetching'}</h2>
			{job.parent_id && <Button type="button" onClick={() => setJob(job.parent_id!)}>parent</Button>}
			<Code language="json">
				{JSON.stringify(job, null, 2)}
			</Code>
			<hr className="my-4" />
			<div className="flex">
				<div className="flex-1">
					<h3 className="text-lg">Steps</h3>
					{data && <Graph
						data={data}
						job={job}
						hoveredEvent={hoveredEvent}
						setHoveredEvent={setHoveredEvent}
					/>}
				</div>

				<div className="max-w-[25vw]">
					<h3 className="text-lg">Events</h3>
					<div onMouseLeave={() => setHoveredEvent([])}>
						{data?.events.map((event, i) => {
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
				</div>
			</div>
		</div>
	)
}

function Graph({
	data,
	job,
	hoveredEvent,
	setHoveredEvent,
}: {
	data: Data,
	job: Task,
	hoveredEvent: number[],
	setHoveredEvent: (event: number[]) => void,
}) {
	const minDate = job.created_at
	const endDate = job.status in refetch ? data.date : job.updated_at

	/** all event durations (in seconds) that are greater than 500ms */
	const intervals: number[] = []
	for (let i = 1; i < data.events.length; i++) {
		const start = data.events[i - 1]!.created_at
		const end = data.events[i]!.created_at
		if (end - start < 0.5) continue
		intervals.push(end - start)
	}
	const average = intervals.reduce((acc, val) => acc + val, 0) / intervals.length
	const stdDev = Math.sqrt(intervals.reduce((acc, val) => acc + (val - average) ** 2, 0) / intervals.length)
	const longDuration = stdDev * 3

	const longIntervals: [number, number][] = []
	for (let i = 0; i < data.events.length - 1; i++) {
		const a = data.events[i]!.created_at
		const b = data.events[i + 1]!.created_at
		if (b - a <= longDuration) continue
		longIntervals.push([a, b])
	}

	const withoutLong = intervals.filter((val) => val <= longDuration)
	const sumWithoutLong = withoutLong.reduce((acc, val) => acc + val, 0)
	const averageWithoutLong = sumWithoutLong / withoutLong.length
	const stdDevWithoutLong = Math.sqrt(withoutLong.reduce((acc, val) => acc + (val - averageWithoutLong) ** 2, 0) / withoutLong.length)
	const maxWithoutLong = Math.max(...withoutLong)

	const adjustedLongEvent = Math.max(maxWithoutLong * 1.5, stdDevWithoutLong * 3)

	function adjustDate(date: number) {
		const before = longIntervals.filter(([, b]) => b <= date)
		return date - before.reduce((acc, [a, b]) => acc + b - a, 0) + before.length * adjustedLongEvent
	}

	const adjustedEnd = adjustDate(endDate)
	const adjustedInterval = adjustedEnd - minDate
	const fullStep = useRef(false)

	return (
		<div className="py-4" onMouseLeave={() => setHoveredEvent([])}>
			<div
				className="relative overflow-x-auto max-w-full z-0"
				onMouseMove={(e) => {
					if (fullStep.current) return
					const x = e.clientX
					const left = e.currentTarget.getBoundingClientRect().left
					const width = e.currentTarget.getBoundingClientRect().width
					const time = minDate + adjustedInterval * ((x - left) / width)
					let min = Infinity
					let i = -1
					for (let j = 0; j < data.events.length; j++) {
						const event = data.events[j]
						const diff = Math.abs(time - adjustDate(event.created_at))
						if (diff < min) {
							min = diff
							i = j
						}
					}
					if (i === -1) return setHoveredEvent([])
					setHoveredEvent([i])
				}}
			>
				{data.steps.map((step, i) => {
					const start = adjustDate(step.created_at)
					const left = (start - minDate) / adjustedInterval * 100
					const end = adjustDate(step.status === 'stalled' || step.status === 'waiting' || step.status === 'running' ? endDate : step.updated_at)
					const width = (end - start) / adjustedInterval * 100
					const isHovered = Boolean(hoveredEvent.length) && hoveredEvent.some(i => cleanEventName(data.events[i].key, job).startsWith(step.step))
					const events = data.events.filter((event) => event.key.startsWith(`step/${job.job}/${step.step}`))
					return (
						<Fragment key={i}>
							{i > 0 && step.discovered_on !== data.steps[i - 1].discovered_on && (
								<div className="relative w-full h-px my-2 z-0 bg-stone-200 dark:bg-stone-800" />
							)}
							<div
								className="relative z-10 transition-all whitespace-nowrap my-1"
								style={{
									left: `${left}%`,
									width: `${width}%`,
								}}
								onMouseEnter={() => {
									fullStep.current = true
									setHoveredEvent(events.map((event) => data.events.indexOf(event)))
								}}
								onMouseLeave={() => {
									fullStep.current = false
								}}
							>
								<StepDisplay
									step={step}
									isHovered={isHovered}
									events={events}
									start={start}
									end={end}
									adjustDate={adjustDate}
									rtl={start > minDate + adjustedInterval * .75}
								/>
							</div>
						</Fragment>
					)
				})}
				{longIntervals.map(([a], i) => {
					const start = adjustDate(a)
					const left = Math.max(0, (start - minDate) / adjustedInterval) * 100
					const end = start + adjustedLongEvent
					const width = (end - start) / adjustedInterval * 100
					return (
						<div
							key={i}
							className="absolute z-0 top-0 bottom-0 transition-all py-0"
							style={{
								left: `${left}%`,
								width: `${width}%`,
								paddingInline: `min(1rem, ${width / 4}%)`,
							}}
						>
							<div
								className="h-full text-stone-200 dark:text-stone-800"
								style={{
									backgroundImage: `repeating-linear-gradient(45deg, transparent, transparent 10px, currentColor 10px, currentColor 11px)`,
								}}
							/>
						</div>
					)
				})}
				{data.events.map((event, i) => {
					const time = adjustDate(event.created_at)
					return (
						<div
							key={i}
							className={clsx(
								"absolute top-0 bottom-0 transition-all pointer-events-none border-l",
								hoveredEvent.includes(i) ? 'z-20' : 'z-0',
								hoveredEvent.includes(i)
									? 'border-fuchsia-500 dark:border-fuchsia-400'
									: 'border-stone-200 dark:border-stone-800'
							)}
							style={{
								left: `min(calc(100% - 1px), ${Math.max(0, (time - minDate) / adjustedInterval) * 100 + '%'})`,
							}}
						/>
					)
				})}
			</div>
		</div>
	)
}


function StepDisplay({
	step,
	isHovered,
	events,
	start,
	end,
	adjustDate,
	rtl,
}: {
	step: Step,
	isHovered: boolean,
	events: Event[],
	start: number,
	end: number,
	adjustDate: (date: number) => number,
	rtl: boolean,
}) {
	const types = events.map(event => event.key.split('/').pop())
	const bgs = []
	for (let i = 1; i <= types.length; i++) {
		const eventStart = adjustDate(events[i - 1].created_at)
		const eventEnd = adjustDate(i === types.length ? end : events[i].created_at)
		const width = (eventEnd - eventStart) / (end - start) * 100
		const left = (eventStart - start) / (end - start) * 100
		const type = types[i]
		bgs.push(
			<div
				key={i}
				className={clsx(
					"absolute top-0 bottom-0 transition-all z-0",
					type === 'error' ? 'bg-red-500 dark:bg-red-800' : type === 'run' ? 'bg-stone-100 dark:bg-stone-900' : type === 'success' ? 'bg-emerald-600 dark:bg-emerald-800' : 'bg-stone-100 dark:bg-stone-900'
				)}
				style={{
					left: `${left}%`,
					width: `${width}%`,
				}}
			/>
		)
	}
	const isSleep = step.step.startsWith('system/sleep')
	return (
		<div
			className={clsx(
				'z-0 transition-all',
				isHovered && 'text-fuchsia-500 dark:text-fuchsia-400',
				isSleep
					? 'bg-stone-100 dark:bg-stone-900'
					: step.status === 'completed' ? 'bg-emerald-600 dark:bg-emerald-800' : step.status === 'failed' ? 'bg-red-500 dark:bg-red-800' : 'bg-stone-100 dark:bg-stone-900'
			)}
			style={{
				height: 'calc(1lh + 1em)',
			}}
		>
			{bgs}
			<span
				className="relative block z-10 whitespace-pre bg-stone-100/20 dark:bg-stone-900/20"
				style={{
					top: '0.5em',
					direction: rtl ? 'rtl' : 'ltr',
				}}
			>
				{` ${step.step} `}
			</span>
		</div>
	)
}