"use client"

import { Table } from "@tanstack/react-table"

import { Button } from "client/components/ui/button"
import { DataTableViewOptions } from "./view-options"

import { statuses } from "./data"
import { DataTableFacetedFilter } from "./faceted-filter"
import { RefreshCw, RefreshCwOff, X } from "lucide-react"
import { Switch } from "client/components/ui/switch"
import { Label } from "client/components/ui/label"

interface DataTableToolbarProps<TData> {
	table: Table<TData>
	liveRefresh?: boolean
	setLiveRefresh?: (value: boolean) => void
}

export function DataTableToolbar<TData>({
	table,
	liveRefresh,
	setLiveRefresh,
}: DataTableToolbarProps<TData>) {
	const isFiltered = table.getState().columnFilters.length > 0

	return (
		<div className="flex items-center justify-between gap-8">
			<div className="flex flex-1 items-center space-x-2">
				{table.getColumn("status") && (
					<DataTableFacetedFilter
						column={table.getColumn("status")}
						title="Status"
						options={statuses}
					/>
				)}
				{/* {table.getColumn("priority") && (
					<DataTableFacetedFilter
						column={table.getColumn("priority")}
						title="Priority"
						options={priorities}
					/>
				)} */}
				{isFiltered && (
					<Button
						variant="ghost"
						onClick={() => table.resetColumnFilters()}
						className="h-8 px-2 lg:px-3"
					>
						Reset
						<X className="ml-2 h-4 w-4" />
					</Button>
				)}
			</div>
			<div className="flex items-center space-x-2">
				<Switch
					checked={liveRefresh}
					onCheckedChange={setLiveRefresh}
					id="live-refresh"
				/>
				<Label htmlFor="live-refresh">
					{liveRefresh ? <RefreshCw className="h-4 w-4" /> : <RefreshCwOff className="h-4 w-4" />}
				</Label>
			</div>
			<DataTableViewOptions table={table} />
		</div>
	)
}