"use client"

import { ColumnDef } from "@tanstack/react-table"

// import { Badge } from "client/components/ui/badge"
import { Checkbox } from "client/components/ui/checkbox"

import { statuses } from "./data"
import { DataTableColumnHeader } from "./column-header"
import type { Task } from "queue"
import { useMemo } from "react"
import { Code } from "client/components/syntax-highlighter"

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
			const lines = JSON.stringify(raw, null, 2).split('\n')
			lines.pop()
			lines.shift()
			const str = lines.map(l => l.slice(2)).join('\n')


			return (
				<Code>
					{str}
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
			return value.includes(row.getValue(id))
		},
	},
	{
		accessorKey: "created_at",
		header: ({ column }) => (
			<DataTableColumnHeader column={column} title="Created At" />
		),
		cell: ({ getValue }) => {
			const value = getValue() as number
			const str = useMemo(() => {
				const date = new Date(value * 1000)
				const formatter = new Intl.DateTimeFormat("en", {
					year: "numeric",
					month: "short",
					day: "2-digit",
					hour: "2-digit",
					minute: "2-digit",
					second: "2-digit",
				})
				return formatter.format(date)
			}, [value])
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
			const str = useMemo(() => {
				const date = new Date(value * 1000)
				const formatter = new Intl.DateTimeFormat("en", {
					year: "numeric",
					month: "short",
					day: "2-digit",
					hour: "2-digit",
					minute: "2-digit",
					second: "2-digit",
				})
				return formatter.format(date)
			}, [value])
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
			const str = useMemo(() => {
				const date = new Date(value * 1000)
				const formatter = new Intl.DateTimeFormat("en", {
					year: "numeric",
					month: "short",
					day: "2-digit",
					hour: "2-digit",
					minute: "2-digit",
					second: "2-digit",
				})
				return formatter.format(date)
			}, [value])
			return (
				<div className="flex items-center">
					<span>{str}</span>
				</div>
			)
		},
	},
	// TODO: when we have a router
	// {
	// 	id: 'go',
	// 	cell: () => {}
	// }
]