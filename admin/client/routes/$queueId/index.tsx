import { useQuery, type UseQueryOptions } from "@tanstack/react-query"
import { createFileRoute, Outlet } from '@tanstack/react-router'
import { Link } from '@tanstack/react-router'
import { Button } from "client/components/ui/button"

export const jobsQueryOpts = (queueId?: string) => ({
  queryKey: [queueId, 'jobs'],
  queryFn: async () => {
    const res = await fetch(`/api/jobs?queue=${queueId}`)
    const json = await res.json()
    return json as string[]
  },
  enabled: Boolean(queueId),
}) satisfies UseQueryOptions

export const Route = createFileRoute('/$queueId/')({
  async loader({ context, params }) {
    const jobs = await context.client.ensureQueryData(jobsQueryOpts(params.queueId))
    return { jobs }
  },
  component: Queue
})

function Queue() {
  const { queueId } = Route.useParams()

  const { data } = useQuery(jobsQueryOpts(queueId))

  return <>
    <div>Queue ID: {queueId}</div>
    <ul className="flex flex-col gap-1">
      {data?.map((job) => (
        <li key={job}>
          <Button asChild>
            <Link to="/$queueId/$jobId" params={{ queueId, jobId: job }} onMouseEnter={() => console.log("/$queueId/$jobId", { queueId, jobId: job })}>{job}</Link>
          </Button>
        </li>
      ))}
    </ul>
    <Outlet />
  </>
}