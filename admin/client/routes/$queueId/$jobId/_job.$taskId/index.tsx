import { createFileRoute, Link, notFound } from '@tanstack/react-router'
import { useJobContext } from "../-components/job-context"
import { useQuery } from "@tanstack/react-query"
import type { Event, Step, Task } from "queue"
import { useState } from "react"
import { Graph } from "client/routes/$queueId/$jobId/_job.$taskId/-components/Graph"
import { Events } from "client/routes/$queueId/$jobId/_job.$taskId/-components/Events"
import { Button } from "client/components/ui/button"
import { Code } from "client/components/syntax-highlighter"

export const Route = createFileRoute('/$queueId/$jobId/_job/$taskId/')({
	component: Task
})

type Data = { steps: Step[], events: Event[], date: number }

const refetch: Record<string, number> = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

function Task() {
	const { queueId, jobId, taskId } = Route.useParams()
	const { tasks } = useJobContext()

	const task = tasks.find(task => String(task.id) === String(taskId))
	if (!task) throw notFound()

	const { data, isFetching } = useQuery({
		queryKey: [queueId, jobId, 'tasks', taskId],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${taskId}`)
			const json = await res.json()
			return json as Data
		},
		refetchInterval: refetch[task.status] ?? false
	})

	const { data: parent } = useQuery({
		queryKey: [queueId, 'task', task.parent_id],
		queryFn: async () => {
			const res = await fetch(`/api/task/${task.parent_id}`)
			const json = await res.json()
			return json as Task
		},
		enabled: !!task.parent_id,
	})

	const [hoveredEvent, setHoveredEvent] = useState<number[]>([])

	return (
		<div className="flex-1">
			<h2 className="text-xl">Task {task.input}{isFetching && ' - fetching'}</h2>
			{parent && <Button asChild>
				<Link to="/$queueId/$jobId/$taskId" params={{ queueId, jobId: parent.job, taskId: parent.id }}>parent</Link>
			</Button>}
			<Code language="json">
				{JSON.stringify(task, null, 2)}
			</Code>
			<hr className="my-4" />
			<div className="flex">
				<div className="flex-1">
					<h3 className="text-lg">Steps</h3>
					{data && <Graph
						data={data}
						job={task}
						hoveredEvent={hoveredEvent}
						setHoveredEvent={setHoveredEvent}
					/>}
				</div>

				<div className="max-w-[25vw]">
					<h3 className="text-lg">Events</h3>
					{data?.events && <Events className="py-4" events={data.events} job={task} hoveredEvent={hoveredEvent} setHoveredEvent={setHoveredEvent} />}
				</div>
			</div>
		</div>
	)
}