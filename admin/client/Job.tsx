import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { Task } from "./Task"

export function Job({ job }: { job: string }) {
	const [task, setTask] = useState<number | null>(null)

	const { data, isFetching } = useQuery({
		queryKey: ['jobs', job],
		queryFn: async () => {
			const res = await fetch(`/api/jobs/${job}`)
			const json = await res.json()
			return json as { id: number, status: string, created_at: number, input: string }[]
		},
		select: (data) => data.sort((a, b) => a.created_at - b.created_at),
		refetchInterval: 5000,
	})

	const jobData = task && data?.find(t => t.id === task)

	return (
		<>
			<h1>{job}{isFetching && ' - fetching'}</h1>
			<div style={{ display: 'flex' }}>
				<ul>
					{data?.map((task) => (
						<li key={task.id}>
							<button type="button" onClick={() => setTask(t => t === task.id ? null : task.id)}>
								{task.status} - {new Date(task.created_at * 1000).toLocaleString()}
								<pre>{JSON.stringify(JSON.parse(task.input), null, 2)}</pre>
							</button>
						</li>
					))}
				</ul>
				{jobData && <Task id={task} job={jobData} />}
			</div>
		</>
	)
}