"use client"

import { ColumnDef } from "@tanstack/react-table"

// import { Badge } from "client/components/ui/badge"
import { Checkbox } from "client/components/ui/checkbox"

import { statuses } from "./data"
import { DataTableColumnHeader } from "./column-header"
import type { Task } from "queue"
import { Code } from "client/components/syntax-highlighter"
import { Link, useParams } from "@tanstack/react-router"
import { Button } from "client/components/ui/button"
import { ArrowRight } from "lucide-react"
import { useFormatDate } from "client/components/use-format-date"


export const columns: ColumnDef<Task>[] = [
	{
		id: "select",
		header: ({ table }) => (
			<Checkbox
				checked={
					table.getIsAllPageRowsSelected() ||
					(table.getIsSomePageRowsSelected() && "indeterminate")
				}
				onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
				aria-label="Select all"
				className="translate-y-[2px]"
			/>
		),
		cell: ({ row }) => (
			<Checkbox
				checked={row.getIsSelected()}
				onCheckedChange={(value) => row.toggleSelected(!!value)}
				aria-label="Select row"
				className="translate-y-[2px]"
			/>
		),
		enableSorting: false,
		enableHiding: false,
	},
	{
		accessorKey: "id",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="ID" />
		),
		cell: ({ row }) => <div className="min-w-[3ch]">{row.getValue("id")}</div>,
	},
	{
		accessorKey: "input",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Input" />
		),
		cell: ({ getValue }) => {
			const raw = JSON.parse(getValue() as string)
			let lines = JSON.stringify(raw, null, 2).split('\n')
			lines.pop()
			lines.shift()
			if (lines.length > 5) {
				lines = lines.slice(0, 5)
				lines.push('  ...')
			}
			const str = lines.map(l => l.slice(2)).join('\n')


			return (
				<Code language="json">
					{str || '{}'}
				</Code>
			)
		},
	},
	{
		accessorKey: "status",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Status" />
		),
		cell: ({ row }) => {
			const status = statuses.find(
				(status) => status.value === row.getValue("status")
			)

			if (!status) {
				return null
			}

			return (
				<div className="flex w-[100px] items-center">
					{status.icon && (
						<status.icon className="mr-2 h-4 w-4 text-muted-foreground" />
					)}
					<span>{status.label}</span>
				</div>
			)
		},
		filterFn: (row, id, value) => {
			return value.includes(row.getValue(id))
		},
	},
	{
		accessorKey: "priority",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Priority" />
		),
		cell: ({ row }) => {
			return (
				<div className="flex items-center">
					<span>{row.getValue('priority')}</span>
				</div>
			)
		},
		filterFn: (row, id, value) => {
			if (!value || typeof value !== 'object') return true
			const cell = row.getValue(id) as number
			if ('min' in value && cell < value.min) return false
			if ('max' in value && cell > value.max) return false
			return true
		},
	},
	{
		accessorKey: "created_at",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Created At" />
		),
		cell: ({ getValue }) => {
			const value = getValue() as number
			const str = useFormatDate(value)
			return (
				<div className="flex items-center">
					<span>{str}</span>
				</div>
			)
		},
	},
	{
		accessorKey: "updated_at",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Updated At" />
		),
		cell: ({ getValue }) => {
			const value = getValue() as number
			const str = useFormatDate(value)
			return (
				<div className="flex items-center">
					<span>{str}</span>
				</div>
			)
		},
	},
	{
		accessorKey: "started_at",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Started At" />
		),
		cell: ({ getValue }) => {
			const value = getValue() as number
			const str = useFormatDate(value)
			return (
				<div className="flex items-center">
					<span>{str}</span>
				</div>
			)
		},
	},
	{
		id: 'go',
		accessorKey: "id",
		header: () => { },
		cell: ({ getValue }) => {
			const { jobId, queueId } = useParams({ from: '/$queueId/$jobId' })
			return (
				<Button asChild size="icon" variant="outline">
					<Link to="/$queueId/$jobId/$taskId" params={{ queueId, jobId, taskId: getValue() as string }}>
						<ArrowRight className="h-4 w-4" />
					</Link>
				</Button>
			)
		}
	}
]