import { Column } from "@tanstack/react-table"

import { Badge } from "client/components/ui/badge"
import { Button } from "client/components/ui/button"
import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "client/components/ui/popover"
import { Separator } from "client/components/ui/separator"
import { CirclePlus } from "lucide-react"
import { Input } from "client/components/ui/input"
import { Label } from "client/components/ui/label"
import { useId, useState } from "react"


interface DataTableIntervalFilterProps<TData, TValue> {
	column?: Column<TData, TValue>
	title?: string
}

type IntervalFilter = {
	min?: number
	max?: number
}

export function DataTableIntervalFilter<TData, TValue>({
	column,
	title,
}: DataTableIntervalFilterProps<TData, TValue>) {
	const filter = (column?.getFilterValue() ?? {}) as IntervalFilter
	const hasMin = 'min' in filter
	const hasMax = 'max' in filter
	const minId = useId()
	const maxId = useId()
	const [open, setOpen] = useState(false)
	return (
		<Popover open={open} onOpenChange={setOpen}>
			<PopoverTrigger asChild>
				<Button variant="outline" size="sm" className="h-8 border-dashed">
					<CirclePlus className="mr-2 h-4 w-4" />
					{title}
					{(hasMin || hasMax) && (
						<>
							<Separator orientation="vertical" className="mx-2 h-4" />
							<div className="flex space-x-1">
								{hasMin && (
									<Badge
										variant="secondary"
										className="rounded-sm px-1 font-normal"
									>
										{'≥'}
										{filter.min}
									</Badge>
								)}
								{hasMax && (
									<Badge
										variant="secondary"
										className="rounded-sm px-1 font-normal"
									>
										{'≤'}
										{filter.max}
									</Badge>
								)}

							</div>
						</>
					)}
				</Button>
			</PopoverTrigger>
			<PopoverContent className="w-[200px] flex flex-col p-0 justify-items-stretch" align="start">
				<div className="flex gap-2 items-center p-2">
					<Label htmlFor={minId}>≥</Label>
					<Input
						type="number"
						placeholder="0"
						name="min-priority"
						id={minId}
						defaultValue={filter.min}
						onChange={(e) => {
							const value = Number(e.target.value)
							filter.min = value
							column?.setFilterValue(filter)
						}}
						max={filter.max}
					/>
				</div>
				<div className="flex gap-2 items-center p-2">
					<Label htmlFor={maxId}>≤</Label>
					<Input
						type="number"
						placeholder="∞"
						name="max-priority"
						id={maxId}
						defaultValue={filter.max}
						onChange={(e) => {
							const value = Number(e.target.value)
							filter.max = value
							column?.setFilterValue(filter)
						}}
						min={filter.min}
					/>
				</div>
				{(hasMax || hasMin) && <>
					<Separator />
					<Button variant="ghost" className="m-2" onClick={() => {
						column?.setFilterValue(undefined)
						setOpen(false)
					}}>
						Reset
					</Button>
				</>}
			</PopoverContent>
		</Popover>
	)
}