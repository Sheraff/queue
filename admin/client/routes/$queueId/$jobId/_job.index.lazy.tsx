import { createLazyFileRoute } from '@tanstack/react-router'
import { DataTable } from "./-components/data-table"
import { useMemo } from "react"
import { columns } from "./-components/columns"
import { useJobContext } from "./-components/job-context"

export const Route = createLazyFileRoute('/$queueId/$jobId/_job/')({
	component: Job
})

function Job() {
	const { tasks } = useJobContext()

	return useMemo(() => <DataTable
		data={tasks}
		columns={columns}
	/>, [tasks])
}