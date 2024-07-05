import { useQuery } from "@tanstack/react-query"
import { useState } from "react"

const refetch = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

export function Task({ id, job, setJob }: { id: number, job: object, setJob: (job: number) => void }) {

	const { data, isFetching } = useQuery({
		queryKey: ['tasks', id],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${id}`)
			const json = await res.json()
			return json as { steps: object[], events: object[], date: number }
		},
		refetchInterval: refetch[job.status] ?? false
	})

	const minDate = data?.steps[0]?.created_at
	const endDate = job.status in refetch ? data?.date : data?.events[data?.events.length - 1]?.created_at
	const interval = endDate ? endDate - minDate : 0

	const [hoveredEvent, setHoveredEvent] = useState<number | null>(null)

	const cleanEventName = (name: string) => name.replace(new RegExp(`^job\\/${job.job}\\/`), '')
		.replace(new RegExp(`^step\\/${job.job}\\/`), '')

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
					{/* TODO: long stretches of time without any event happening should be collapsed (or collapsable) */}
					<div style={{ maxWidth: '100%', position: 'relative', zIndex: 0 }}>
						{data?.steps.map((step, i) => {
							const left = (step.created_at - minDate) / interval * 100
							const end = step.status === 'stalled' || step.status === 'waiting' || step.status === 'running' ? endDate : step.updated_at
							const width = (end - step.created_at) / interval * 100
							const isHovered = hoveredEvent !== null && cleanEventName(data?.events[hoveredEvent].key).startsWith(step.step)
							const relatedEvents = data?.events.filter(event => event.key.startsWith(`step/${job.job}/${step.step}`))
							return (
								<div key={i} style={{
									left: `${left}%`,
									width: `${width}%`,
									position: 'relative',
									whiteSpace: 'nowrap',
									zIndex: 1,
								}}>
									<Step step={step} isHovered={isHovered} events={relatedEvents} start={step.created_at} end={end} />
								</div>
							)
						})}
						{data?.events.map((event, i) => {
							return (
								<div key={i} style={{
									left: Math.max(0, (event.created_at - minDate) / interval) * 100 + '%',
									position: 'absolute',
									top: 0,
									bottom: 0,
									borderLeft: hoveredEvent === i ? '1px solid magenta' : '1px solid lightgray',
									zIndex: hoveredEvent === i ? 2 : 0
								}} />
							)
						})}
					</div>
				</div>

				<div style={{ maxWidth: '25vw' }}>
					<h3>Events</h3>
					<div onMouseLeave={() => setHoveredEvent(null)}>
						{data?.events.map((event, i) => {
							const name = cleanEventName(event.key)
							return (
								<div key={i} style={{ backgroundColor: i === hoveredEvent ? 'lightblue' : 'transparent' }} onMouseEnter={() => setHoveredEvent(i)}>
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


function Step({ step, isHovered, events, start, end }: { step: object, isHovered: boolean, events: object[] }) {
	const types = events.map(event => event.key.split('/').pop())
	const bgs = []
	for (let i = 1; i <= types.length; i++) {
		// TODO: this is kinda wrong. If we have the events [run, error, run],
		// the interval between the first two should be red (this run had the error),
		// and the second two should be gray (this is the backoff delay).
		const prev = types[i - 1]
		const event = events[i - 1]
		const next = i === types.length ? end : events[i].created_at
		const width = (next - event.created_at) / (end - start) * 100
		const left = (event.created_at - start) / (end - start) * 100
		const color = prev === 'error' ? 'red' : prev === 'run' ? 'gray' : prev === 'success' ? 'green' : 'lightgray'
		bgs.push(
			<div key={i} style={{ backgroundColor: color, position: 'absolute', top: 0, bottom: 0, left: `${left}%`, width: `${width}%`, zIndex: 0 }} />
		)
	}
	return (
		<div
			style={{
				zIndex: 0,
				backgroundColor: isHovered ? 'pink' : step.status === 'completed' ? 'green' : step.status === 'error' ? 'red' : 'gray',
				padding: '0.5em 0',
			}}
		>
			{bgs}
			<span style={{ padding: '0 0.5em', position: 'relative', zIndex: 1 }}>{step.step}</span>
		</div>
	)
}