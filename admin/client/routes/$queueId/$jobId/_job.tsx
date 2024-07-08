import { createFileRoute } from '@tanstack/react-router'
import { jobQueryOpts } from "./-components/job-context"

export const Route = createFileRoute('/$queueId/$jobId/_job')({
	loader: async ({ context, params }) => {
		const tasks = await context.client.ensureQueryData(jobQueryOpts(params.queueId, params.jobId, true))
		return { tasks }
	},
})
