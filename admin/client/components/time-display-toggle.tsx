import { Calendar, Timer } from "lucide-react"

import { Button } from "client/components/ui/button"
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuTrigger,
} from "client/components/ui/dropdown-menu"
import { useTimeDisplay } from "./time-display-provider"

export function TimeDisplayToggle() {
	const [state, setState] = useTimeDisplay()

	return (
		<DropdownMenu>
			<DropdownMenuTrigger asChild>
				<Button variant="outline" size="icon">
					{state === 'relative' && <Timer className="h-[1.2rem] w-[1.2rem]" />}
					{state === 'absolute' && <Calendar className="h-[1.2rem] w-[1.2rem]" />}
					<span className="sr-only">Toggle time display</span>
				</Button>
			</DropdownMenuTrigger>
			<DropdownMenuContent align="end">
				<DropdownMenuItem onClick={() => setState("absolute")}>
					Absolute
				</DropdownMenuItem>
				<DropdownMenuItem onClick={() => setState("relative")}>
					Relative
				</DropdownMenuItem>
			</DropdownMenuContent>
		</DropdownMenu>
	)
}
