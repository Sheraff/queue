import { useQuery } from "@tanstack/react-query"
import { memo, useState } from "react"
import { TaskPage } from "./Task"
import { Button } from "client/components/ui/button"
import type { Task } from "queue"
import { Code } from "client/components/Code"

const MemoTask = memo(TaskPage)

export function Job({ job }: { job: string }) {
	const [task, setTask] = useState<number | null>(null)

	const { data, isFetching } = useQuery({
		queryKey: ['jobs', job],
		queryFn: async () => {
			const res = await fetch(`/api/jobs/${job}`)
			const json = await res.json()
			return json as Task[]
		},
		select: (data) => data.sort((a, b) => a.created_at - b.created_at),
		refetchInterval: 5000,
	})

	const jobData = task && data?.find(t => t.id === task)

	return (
		<>
			<h1 className="text-2xl px-2">{job}{isFetching && ' - fetching'}</h1>
			<div className="flex gap-4 p-2">
				<ul className="flex flex-col gap-1">
					{data?.map((task) => (
						<li key={task.id}>
							<Button variant="outline" onClick={() => setTask(t => t === task.id ? null : task.id)}>
								{task.status} - {new Date(task.created_at * 1000).toLocaleString()}
								<Code className="text-left">{JSON.stringify(JSON.parse(task.input), null, 2)}</Code>
							</Button>
						</li>
					))}
				</ul>
				{jobData && <MemoTask id={task} job={jobData} setJob={setTask} key={`job${job}task${jobData.id}`} />}
			</div>
		</>
	)
}