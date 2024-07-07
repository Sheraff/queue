import { useQuery } from "@tanstack/react-query"
import { memo, useState } from "react"
import { TaskPage } from "../tasks/Task"
import { Button } from "client/components/ui/button"
import type { Task } from "queue"

import { DataTable } from "./data-table"
import { columns } from "./columns"


const MemoTask = memo(TaskPage)


export function Job({ job }: { job: string }) {
	const [task, setTask] = useState<number | null>(null)
	const [liveRefresh, setLiveRefresh] = useState(true)

	const { data, isFetching, refetch } = useQuery({
		queryKey: ['jobs', job],
		queryFn: async () => {
			const res = await fetch(`/api/jobs/${job}`)
			const json = await res.json()
			return json as Task[]
		},
		select: (data) => data.sort((a, b) => b.created_at - a.created_at),
		refetchInterval: liveRefresh || task ? 5000 : false,
	})

	const jobData = task && data?.find(t => t.id === task)

	return (
		<>
			<h1 className="text-2xl px-2">{job}{isFetching && ' - fetching'}</h1>
			{task && <Button onClick={() => setTask(null)}>Back</Button>}
			<div className="p-2 mt-4">
				{!task && data && <DataTable
					data={data}
					columns={columns}
					onRowClick={(row) => setTask(row.getValue('id'))}
					liveRefresh={liveRefresh}
					setLiveRefresh={(value) => {
						setLiveRefresh(value)
						if (value) refetch()
					}}
				/>}
				{jobData && <MemoTask
					id={task}
					job={jobData}
					setJob={setTask}
					key={`job${job}task${jobData.id}`}
				/>}
			</div>
		</>
	)
}