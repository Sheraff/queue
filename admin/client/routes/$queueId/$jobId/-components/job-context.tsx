import type { UseQueryOptions } from "@tanstack/react-query"
import type { Task } from "queue"
import { createContext, useContext } from "react"

export const jobQueryOpts = (queueId: string, job: string, liveRefresh: boolean) => ({
	queryKey: [queueId, 'jobs', job],
	queryFn: async () => {
		const res = await fetch(`/api/jobs/${job}`)
		const json = await res.json()
		return json
	},
	select: (data) => data.sort((a, b) => b.created_at - a.created_at),
	refetchInterval: liveRefresh ? 5000 : false,
}) satisfies UseQueryOptions<Task[]>

type JobContextValue = {
	tasks: Task[],
	liveRefresh: boolean,
	setLiveRefresh: (value: boolean) => void,
}

export const JobContext = createContext<JobContextValue | null>(null)

export function useJobContext() {
	const context = useContext(JobContext)
	if (context === null) throw new Error('useJobContext must be used inside a JobContextProvider')
	return context
}