import { CircleCheckBig, CircleDashed, CircleDot, CircleDotDashed, CircleSlash, CircleX } from "lucide-react"

export const statuses = [
	{
		description: "task is ready to be picked up",
		value: 'pending',
		label: "Pending",
		icon: CircleDashed
	},
	{
		description: "task is being processed, do not pick up",
		value: 'running',
		label: "Running",
		icon: CircleDot
	},
	{
		description: "task is waiting for a timer (retries, debounce, throttle, ...)",
		value: 'stalled',
		label: "Stalled",
		icon: CircleDotDashed
	},
	{
		description: "task finished, data is the successful result",
		value: 'completed',
		label: "Completed",
		icon: CircleCheckBig,
	},
	{
		description: "task failed, data is the error",
		value: 'failed',
		label: "Failed",
		icon: CircleX
	},
	{
		description: "task was cancelled, data is the reason",
		value: 'cancelled',
		label: "Cancelled",
		icon: CircleSlash
	},
]
