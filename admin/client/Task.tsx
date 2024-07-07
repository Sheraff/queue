import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import type { Step, Event } from 'queue'

type Data = { steps: Step[], events: Event[], date: number }

const refetch = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

const red = '#ef4444'
const green = '#86efac'
const bgGray = '#f1f5f9'
const borderGray = '#e2e8f0'
const blue = '#93c5fd'
const accent = '#c026d3'

const cleanEventName = (name: string, job: { job: string }) => name.replace(new RegExp(`^job\\/${job.job}\\/`), '')
	.replace(new RegExp(`^step\\/${job.job}\\/`), '')

export function Task({ id, job, setJob }: { id: number, job: object, setJob: (job: number) => void }) {

	const { data, isFetching } = useQuery({
		queryKey: ['tasks', id],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${id}`)
			const json = await res.json()
			return json as Data
		},
		refetchInterval: refetch[job.status] ?? false
	})

	const [hoveredEvent, setHoveredEvent] = useState<number | null>(null)

	return (
		<div style={{ flex: 1 }}>
			<h2>Task {job.input}{isFetching && ' - fetching'}</h2>
			{job.parent_id && <button type="button" onClick={() => setJob(job.parent_id)}>parent</button>}
			<pre>
				{JSON.stringify(job, null, 2)}
			</pre>
			<hr />
			<div style={{ display: 'flex' }}>
				<div style={{ flex: 1 }}>
					<h3>Steps</h3>
					{data && <Graph data={data} job={job} hoveredEvent={hoveredEvent} />}
				</div>

				<div style={{ maxWidth: '25vw' }}>
					<h3>Events</h3>
					<div onMouseLeave={() => setHoveredEvent(null)}>
						{data?.events.map((event, i) => {
							const name = cleanEventName(event.key, job)
							return (
								<div key={i} style={{ backgroundColor: i === hoveredEvent ? blue : 'transparent' }} onMouseEnter={() => setHoveredEvent(i)}>
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

function Graph({ data, job, hoveredEvent }: { data: Data, job: object, hoveredEvent: number | null }) {
	const minDate = data.steps[0]?.created_at
	const endDate = job.status in refetch ? data.date : Math.max(data.events[data.events.length - 1].created_at, data.steps[data.steps.length - 1].updated_at)

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

	const adjustedLongEvent = stdDev * 3

	function adjustDate(date: number) {
		const before = longIntervals.filter(([, b]) => b <= date)
		return date - before.reduce((acc, [a, b]) => acc + b - a, 0) + before.length * adjustedLongEvent
	}

	const adjustedEnd = adjustDate(endDate)
	const adjustedInterval = adjustedEnd - minDate

	return (
		<div style={{ padding: '1em' }}>
			<div style={{ maxWidth: '100%', position: 'relative', zIndex: 0, overflowX: 'auto' }}>
				{data.steps.map((step, i) => {
					const start = adjustDate(step.created_at)
					const left = (start - minDate) / adjustedInterval * 100
					const end = adjustDate(step.status === 'stalled' || step.status === 'waiting' || step.status === 'running' ? endDate : step.updated_at)
					const width = (end - start) / adjustedInterval * 100
					const isHovered = hoveredEvent !== null && cleanEventName(data.events[hoveredEvent].key, job).startsWith(step.step)
					const events = data.events.reduce((acc, event, i) => {
						if (!event.key.startsWith(`step/${job.job}/${step.step}`)) return acc
						acc.push(event)
						return acc
					}, [] as Event[])
					return (
						<div key={i} style={{
							left: `${left}%`,
							width: `${width}%`,
							position: 'relative',
							whiteSpace: 'nowrap',
							zIndex: 1,
						}}>
							<Step
								step={step}
								isHovered={isHovered}
								events={events}
								start={start}
								end={end}
								adjustDate={adjustDate}
								rtl={start > minDate + adjustedInterval * .75}
							/>
						</div>
					)
				})}
				{longIntervals.map(([a, b], i) => {
					const start = adjustDate(a)
					const left = Math.max(0, (start - minDate) / adjustedInterval) * 100
					const end = start + adjustedLongEvent
					const width = (end - start) / adjustedInterval * 100
					return (
						<div key={i} style={{
							left: `${left}%`,
							width: `${width}%`,
							position: 'absolute',
							top: 0,
							bottom: 0,
							zIndex: 0,
							padding: `0 min(1rem, ${width / 4}%)`,
						}}>
							<div style={{
								backgroundImage: `repeating-linear-gradient(45deg, transparent, transparent 5px, ${borderGray} 5px, ${borderGray} 6px)`,
								height: '100%',
							}} />
						</div>
					)
				})}
				{data.events.map((event, i) => {
					const time = adjustDate(event.created_at)
					return (
						<div key={i} style={{
							left: `min(calc(100% - 1px), ${Math.max(0, (time - minDate) / adjustedInterval) * 100 + '%'})`,
							position: 'absolute',
							top: 0,
							bottom: 0,
							borderLeft: hoveredEvent === i ? `1px solid ${accent}` : `1px solid ${borderGray}`,
							zIndex: hoveredEvent === i ? 2 : 0
						}} />
					)
				})}
			</div>
		</div>
	)
}


function Step({
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
		const color = type === 'error' ? red : type === 'run' ? bgGray : type === 'success' ? green : bgGray
		bgs.push(
			<div key={i} style={{ backgroundColor: color, position: 'absolute', top: 0, bottom: 0, left: `${left}%`, width: `${width}%`, zIndex: 0 }} />
		)
	}
	const isSleep = step.step.startsWith('system/sleep#')
	return (
		<div
			style={{
				zIndex: 0,
				backgroundColor: isSleep ? bgGray : step.status === 'completed' ? green : step.status === 'failed' ? red : bgGray,
				color: isHovered ? accent : 'black',
				height: 'calc(1lh + 1em)',
			}}
		>
			{bgs}
			<span style={{
				display: 'block',
				top: '0.5em',
				whiteSpace: 'pre',
				position: 'relative',
				zIndex: 1,
				direction: rtl ? 'rtl' : 'ltr',
			}}>
				{` ${step.step} `}
			</span>
		</div>
	)
}