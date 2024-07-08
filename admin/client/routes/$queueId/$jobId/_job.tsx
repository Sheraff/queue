import { createFileRoute, Link, Outlet, useParams } from '@tanstack/react-router'
import { JobContext, jobQueryOpts } from "./-components/job-context"
import { useSuspenseQuery } from "@tanstack/react-query"
import { useState } from "react"

export const Route = createFileRoute('/$queueId/$jobId/_job')({
	loader: async ({ context, params }) => {
		const tasks = await context.client.ensureQueryData(jobQueryOpts(params.queueId, params.jobId, true))
		return { tasks }
	},
	component: Layout
})

function Layout() {
	const { jobId, queueId } = Route.useParams()
	const { taskId } = useParams({ strict: false })
	const [liveRefresh, setLiveRefresh] = useState(true)

	const { data: tasks, refetch, isFetching } = useSuspenseQuery(jobQueryOpts(queueId, jobId, Boolean(liveRefresh || taskId)))

	return (
		<>
			<h1 className="text-2xl p-8 pb-0">
				<Link to="/$queueId/$jobId" params={{ jobId, queueId }}>{jobId}</Link>
				{isFetching && ' - fetching'}
			</h1>
			<div className="p-8">
				<JobContext.Provider value={{
					tasks,
					liveRefresh,
					setLiveRefresh: (value) => {
						setLiveRefresh(value)
						if (value) refetch()
					}
				}}>
					<Outlet />
				</JobContext.Provider>
			</div>
		</>
	)
}