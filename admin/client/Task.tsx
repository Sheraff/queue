import { useQuery } from "@tanstack/react-query"

const refetch = {
	pending: 2000,
	running: 300,
	stalled: 10000,
}

export function Task({ id, job }: { id: number, job: object }) {

	const { data, isFetching } = useQuery({
		queryKey: ['tasks', id],
		queryFn: async () => {
			const res = await fetch(`/api/tasks/${id}`)
			const json = await res.json()
			return json as { steps: object[], events: object[] }
		},
		refetchInterval: refetch[job.status] ?? false
	})

	return (
		<div>
			<h2>Task {job.input}{isFetching && ' - fetching'}</h2>
			<pre>
				{JSON.stringify(job, null, 2)}
			</pre>
			<hr />
			<div style={{ display: 'flex' }}>
				<div>
					<h3>Steps</h3>
					<pre>
						{JSON.stringify(data?.steps.sort((a, b) => a.created_at - b.created_at), null, 2)}
					</pre>
				</div>

				<div>
					<h3>Events</h3>
					<pre>
						{JSON.stringify(data?.events.sort((a, b) => a.created_at - b.created_at), null, 2)}
					</pre>
				</div>
			</div>
		</div>
	)
}