import { useSuspenseQuery, type QueryClient, type UseQueryOptions } from "@tanstack/react-query"
import { createRootRouteWithContext, Link, Outlet, redirect, useNavigate, useParams } from '@tanstack/react-router'
import { ModeToggle } from "client/components/mode-toggle"
import { TimeDisplayToggle } from "client/components/time-display-toggle"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue, } from "client/components/ui/select"
import { TanStackRouterDevtools } from '@tanstack/router-devtools'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { Button } from "client/components/ui/button"

const queueOptions = {
	queryKey: ['queues'],
	queryFn: async () => {
		const res = await fetch('/api/queues')
		const json = await res.json()
		return json as string[]
	}
} satisfies UseQueryOptions

function SelectQueue() {
	const { data } = useSuspenseQuery(queueOptions)
	const { queueId } = useParams({ strict: false })
	const nav = useNavigate()
	return (
		<Select value={queueId} onValueChange={queueId => nav({ to: "/$queueId", params: { queueId } })}>
			<SelectTrigger className="w-[180px]">
				<SelectValue placeholder="Select a queue" />
			</SelectTrigger>
			<SelectContent>
				{data.map((queue) => (
					<SelectItem key={queue} value={queue}>{queue}</SelectItem>
				))}
			</SelectContent>
		</Select>
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
			<nav className="flex p-4 gap-2">
				<SelectQueue />
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