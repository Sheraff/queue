import { createLazyFileRoute, Link, notFound } from '@tanstack/react-router'
import { useJobContext } from "../-components/job-context"
import { useQuery } from "@tanstack/react-query"
import type { Event, Log, Step, Task } from "queue"
import { useMemo, useState } from "react"
import { Graph } from "client/routes/$queueId/$jobId/_job.$taskId/-components/Graph"
import { Events } from "client/routes/$queueId/$jobId/_job.$taskId/-components/Events"
import { Button } from "client/components/ui/button"
import { Code } from "client/components/syntax-highlighter"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "client/components/ui/tabs"
import { Braces, CodeXml, Database } from "lucide-react"
import { Card, CardContent } from "client/components/ui/card"

export const Route = createLazyFileRoute('/$queueId/$jobId/_job/$taskId/')({
	component: Task
})

type Data = { steps: Step[], events: Event[], logs: Log[], date: number }

const refetch: Record<string, number> = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

function Task() {
	const { queueId, jobId, taskId } = Route.useParams()
	const { tasks } = useJobContext()

	const task = tasks.find(task => String(task.id) === taskId)
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

	const { data: source } = useQuery({
		queryKey: [queueId, 'job-source', task.job],
		queryFn: async () => {
			const res = await fetch(`/api/source/${task.job}`)
			const text = await res.text()
			return text.trim()
		},
		staleTime: Infinity,
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

	const all = useMemo(() => data && [
		...data.events,
		...data.logs.filter(l => l.system)
	].sort((a, b) => a.created_at - b.created_at), [data])

	return (
		<div className="flex-1">
			<h2 className="text-xl">Task {task.input}{isFetching && ' - fetching'}</h2>
			{task.parent_id && !parent && <Button disabled>parent</Button>}
			{parent && <Button asChild>
				<Link to="/$queueId/$jobId/$taskId" params={{ queueId, jobId: parent.job, taskId: String(parent.id) }}>parent</Link>
			</Button>}
			<Tabs defaultValue="state" className="w-full my-4">
				<TabsList>
					<TabsTrigger className="gap-2" value="state"><Database className="h-4 w-4" />Stored state</TabsTrigger>
					<TabsTrigger className="gap-2" value="payload"><Braces className="h-4 w-4" />Input payload</TabsTrigger>
					{source && <TabsTrigger className="gap-2" value="source"><CodeXml className="h-4 w-4" />Job source</TabsTrigger>}
				</TabsList>
				<Card className="mt-4">
					<CardContent>
						<TabsContent value="state">
							<Code language="json">
								{JSON.stringify(task, null, 2)}
							</Code>
						</TabsContent>
						<TabsContent value="payload">
							<Code language="json">
								{JSON.stringify(JSON.parse(task.input), null, 2)}
							</Code>
						</TabsContent>
						{source && (
							<TabsContent value="source">
								<Code language="javascript" showLineNumbers>
									{source}
								</Code>
							</TabsContent>
						)}
					</CardContent>
				</Card>
			</Tabs>
			<div className="flex">
				<div className="flex-1">
					<h3 className="text-lg">Steps</h3>
					{data && <Graph
						data={data}
						all={all!}
						job={task}
						hoveredEvent={hoveredEvent}
						setHoveredEvent={setHoveredEvent}
					/>}
				</div>

				<div className="max-w-[25vw]">
					<h3 className="text-lg">Events</h3>
					{all && <Events className="py-4" events={all} job={task} hoveredEvent={hoveredEvent} setHoveredEvent={setHoveredEvent} />}
				</div>
			</div>
		</div>
	)
}