import { useQuery } from "@tanstack/react-query"

export function Task({ id, job }: { id: number, job: object }) {

	const { data } = useQuery({
		queryKey: ['tasks', id],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${id}`)
			const json = await res.json()
			return json as { steps: object[] }
		},
		refetchInterval: job.status === 'completed' ? false : 1000
	})

	return (
		<div>
			<pre>
				{JSON.stringify(job, null, 2)}
			</pre>
			<hr />
			<pre>
				{JSON.stringify(data?.steps.sort((a, b) => a.created_at - b.created_at), null, 2)}
			</pre>
		</div>
	)
}