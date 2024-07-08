import { type UseQueryOptions } from "@tanstack/react-query"
import { createFileRoute } from '@tanstack/react-router'

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
})
