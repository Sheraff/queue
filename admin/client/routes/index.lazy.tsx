import { useSuspenseQuery } from "@tanstack/react-query"
import { createLazyFileRoute, Link } from '@tanstack/react-router'
import { Button } from "client/components/ui/button"
import { queueOptions } from "client/routes/__root"

export const Route = createLazyFileRoute('/')({
	component: Index,
})

function Index() {
	const { data } = useSuspenseQuery(queueOptions)

	return (
		<div className="p-8">
			<h3>Welcome Home!</h3>
			<ul className="mt-8 flex flex-col gap-2">
				{data.map((queue) => (
					<li key={queue}>
						<Button asChild>
							<Link to="/$queueId" params={{ queueId: queue }}>{queue}</Link>
						</Button>
					</li>
				))}
			</ul>
		</div>
	)
}