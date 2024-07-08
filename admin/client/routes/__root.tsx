import { useQuery, useSuspenseQuery, type QueryClient, type UseQueryOptions } from "@tanstack/react-query"
import { createRootRouteWithContext, Link, Outlet, redirect, useNavigate, useParams } from '@tanstack/react-router'
import { ModeToggle } from "client/components/mode-toggle"
import { TimeDisplayToggle } from "client/components/time-display-toggle"
import { TanStackRouterDevtools } from '@tanstack/router-devtools'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { Button } from "client/components/ui/button"
import { jobsQueryOpts } from "client/routes/$queueId"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "client/components/ui/dropdown-menu"
import { ChevronRight } from "lucide-react"

const queueOptions = {
	queryKey: ['queues'],
	queryFn: async () => {
		const res = await fetch('/api/queues')
		const json = await res.json()
		return json as string[]
	}
} satisfies UseQueryOptions

function SelectNav() {
	const { data } = useSuspenseQuery(queueOptions)
	const { queueId, jobId } = useParams({ strict: false })
	const nav = useNavigate()
	const { data: jobs } = useQuery(jobsQueryOpts(queueId))
	return (
		<>
			<DropdownMenu>
				<DropdownMenuTrigger asChild>
					<Button variant="ghost">
						{queueId ? queueId : "Select a queue"}
					</Button>
				</DropdownMenuTrigger>
				<DropdownMenuContent align="start">
					{data.map((queue) => (
						<DropdownMenuItem key={queue} onClick={() => nav({ to: "/$queueId", params: { queueId: queue } })}>
							{queue}
						</DropdownMenuItem>
					))}
				</DropdownMenuContent>
			</DropdownMenu>
			{queueId && <ChevronRight className="h-4 w-4" />}
			{queueId && (
				<DropdownMenu>
					<DropdownMenuTrigger asChild>
						<Button variant="ghost">
							{jobId ? jobId : "Select a job"}
						</Button>
					</DropdownMenuTrigger>
					<DropdownMenuContent align="start">
						{jobs?.map((job) => (
							<DropdownMenuItem key={job} onClick={() => nav({ to: "/$queueId/$jobId", params: { queueId, jobId: job } })}>
								{job}
							</DropdownMenuItem>
						))}
						{jobId && (
							<>
								<DropdownMenuSeparator />
								<DropdownMenuItem onClick={() => nav({ to: "/$queueId", params: { queueId } })}>
									See all jobs
								</DropdownMenuItem>
							</>
						)}
					</DropdownMenuContent>
				</DropdownMenu>
			)}
		</>
	)
}

export const Route = createRootRouteWithContext<{ client: QueryClient }>()({
	loader: async ({ context, params }) => {
		const data = await context.client.ensureQueryData(queueOptions)
		if ('queueId' in params) return
		throw redirect({
			to: "/$queueId",
			params: { queueId: data[0] },
		})
	},
	component: () => (
		<>
			<nav className="flex p-4 gap-2 items-center">
				<SelectNav />
				<div className="flex-1" />
				<TimeDisplayToggle />
				<ModeToggle />
			</nav>
			<hr />
			<Outlet />
			<hr className="mt-8" />
			<footer className="flex justify-center flex-wrap gap-2 p-8 text-center text-xs text-gray-500">
				<Button asChild variant="link">
					<Link to="/about">About</Link>
				</Button>
			</footer>
			<TanStackRouterDevtools />
			<ReactQueryDevtools />
		</>
	),
})