import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { Task } from "./Task"

export function Job({ job }: { job: string }) {
	const [task, setTask] = useState<number | null>(null)

	const { data } = useQuery({
		queryKey: ['jobs', job],
		queryFn: async () => {
			const res = await fetch(`/api/jobs/${job}`)
			const json = await res.json()
			return json as { id: number, status: string, created_at: number, input: string }[]
		},
		select: (data) => data.sort((a, b) => a.created_at - b.created_at),
		refetchInterval: 1000
	})

	return (
		<>
			<h1>{job}</h1>
			<div style={{ display: 'flex' }}>
				<ul>
					{data?.map((task) => (
						<li key={task.id}>
							<button type="button" onClick={() => setTask(task.id)}>
								{task.status} - {new Date(task.created_at * 1000).toLocaleString()}
								<pre>{JSON.stringify(JSON.parse(task.input), null, 2)}</pre>
							</button>
						</li>
					))}
				</ul>
				{task && <Task id={task} job={data?.find(t => t.id === task)} />}
			</div>
		</>
	)
}