import { useQuery } from "@tanstack/react-query"
import { createLazyFileRoute, Outlet } from '@tanstack/react-router'
import { Link } from '@tanstack/react-router'
import { Button } from "client/components/ui/button"
import { jobsQueryOpts } from "client/routes/$queueId"

export const Route = createLazyFileRoute('/$queueId/')({
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