import { Fragment, useLayoutEffect, useMemo, useRef, useState, type ReactElement } from "react"
import type { Step, Event, Task, Log, SystemLog } from 'queue'
import clsx from "clsx"
import { CircleCheckBig, CircleDashed, CircleX, Clock, Workflow } from "lucide-react"
import { Code } from "client/components/syntax-highlighter"
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetTrigger } from "client/components/ui/sheet"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "client/components/ui/tabs"
import { Card, CardContent, CardHeader } from "client/components/ui/card"
import { Separator } from "client/components/ui/separator"
import { useFormatDate } from "client/components/use-format-date"


const ACTIVE_STATUSES = [
	'pending',
	'running',
	'stalled',
]

export function Graph({
	data,
	all,
	job,
	hoveredEvent,
	setHoveredEvent,
}: {
	data: { steps: Step[], events: Event[], logs: Log[], date: number },
	all: (Event | SystemLog)[],
	job: Task,
	hoveredEvent: number[],
	setHoveredEvent: (event: number[]) => void,
}) {

	const minDate = job.created_at
	const endDate = ACTIVE_STATUSES.includes(job.status) ? data.date : job.updated_at

	/** all event durations (in seconds) that are greater than 500ms */
	const intervals: number[] = []
	for (let i = 1; i < all.length; i++) {
		const start = all[i - 1]!.created_at
		const end = all[i]!.created_at
		if (end - start < 0.5) continue
		intervals.push(end - start)
	}
	const average = intervals.reduce((acc, val) => acc + val, 0) / intervals.length
	const stdDev = Math.sqrt(intervals.reduce((acc, val) => acc + (val - average) ** 2, 0) / intervals.length)
	const longDuration = average + stdDev * 2

	const longIntervals: [number, number][] = []
	for (let i = 0; i < all.length - 1; i++) {
		const a = all[i]!.created_at
		const b = all[i + 1]!.created_at
		if (b - a <= longDuration) continue
		longIntervals.push([a, b])
	}

	const withoutLong = intervals.filter((val) => val <= longDuration)
	const sumWithoutLong = withoutLong.reduce((acc, val) => acc + val, 0)
	const averageWithoutLong = sumWithoutLong / withoutLong.length
	const stdDevWithoutLong = Math.sqrt(withoutLong.reduce((acc, val) => acc + (val - averageWithoutLong) ** 2, 0) / withoutLong.length)
	const maxWithoutLong = Math.max(...withoutLong)

	const adjustedLongEvent = Math.max(maxWithoutLong * 1.5, averageWithoutLong + stdDevWithoutLong * 2)

	function adjustDate(date: number) {
		const before = longIntervals.filter(([, b]) => b <= date)
		return date - before.reduce((acc, [a, b]) => acc + b - a, 0) + before.length * adjustedLongEvent
	}

	const adjustedEnd = adjustDate(endDate)
	const adjustedInterval = adjustedEnd - minDate
	// const eventDensity = (averageWithoutLong - 1 * stdDevWithoutLong) / adjustedInterval * 100
	const wAdjust = Math.max(adjustedInterval / (averageWithoutLong - 1 * stdDevWithoutLong), 100) // <= an interval of the size "avg - stdDev * 1" should be at least 1% of the width

	const fullStep = useRef(false)

	// stick to the right side
	const scrollable = useRef<HTMLDivElement>(null)
	const lastRender = useRef(false)
	useLayoutEffect(() => {
		const el = scrollable.current
		if (!el) return

		if (lastRender.current) {
			el.scrollTo({ left: el.scrollWidth - el.clientWidth, behavior: 'smooth' })
		}

		return () => {
			const b = el.scrollWidth - el.clientWidth
			if (!b) {
				lastRender.current = false
			} else {
				const a = el.scrollLeft
				lastRender.current = Math.abs(a - b) < 20
			}
		}
	})

	const [sheetData, setSheetData] = useState<{ step: Step, key: string } | null>(null)
	const sheetEvents = useMemo(() => sheetData && data.logs.filter((event) => event.key === sheetData.key), [sheetData, data.logs])

	return (
		<div
			ref={scrollable}
			className="py-4 overflow-x-auto"
			onMouseLeave={() => setHoveredEvent([])}
		>
			<div
				className="relative z-0 transition-all"
				style={{
					width: `${wAdjust}%`,
				}}
				onMouseMove={(e) => {
					if (fullStep.current) return
					const x = e.clientX
					const left = e.currentTarget.getBoundingClientRect().left
					const width = e.currentTarget.getBoundingClientRect().width
					const time = minDate + adjustedInterval * ((x - left) / width)
					let min = Infinity
					let i = -1
					for (let j = 0; j < all.length; j++) {
						const event = all[j]
						const diff = Math.abs(time - adjustDate(event.created_at))
						if (diff < min) {
							min = diff
							i = j
						}
					}
					if (i === -1) return setHoveredEvent([])
					setHoveredEvent([i])
				}}
			>
				<Sheet>
					{sheetData && (
						<StepDetails step={sheetData.step} logs={sheetEvents!} />
					)}

					{data.steps.map((step, i) => {
						const start = adjustDate(step.created_at)
						const left = (start - minDate) / adjustedInterval * 100
						const end = Math.min(adjustedEnd, adjustDate(step.status === 'stalled' || step.status === 'waiting' || step.status === 'running' ? endDate : step.updated_at))
						const width = (end - start) / adjustedInterval * 100
						const key = `step/${job.job}/${step.step}`
						const isHovered = Boolean(hoveredEvent.length) && hoveredEvent.some(i => all[i].key === key)
						const logs = data.logs.filter((log) => log.system && log.key === key) as SystemLog[]
						return (
							<Fragment key={i}>
								{i > 0 && step.discovered_on !== data.steps[i - 1].discovered_on && (
									<div className="relative w-full h-px my-2 z-0 bg-stone-200 dark:bg-stone-800" />
								)}
								<SheetTrigger onPointerEnter={() => setSheetData({ step, key })} asChild>
									<div
										className="block relative z-10 transition-all whitespace-nowrap my-1 cursor-pointer"
										style={{
											left: `${left}%`,
											width: `${width}%`,
										}}
										onMouseEnter={() => {
											fullStep.current = true
											setHoveredEvent(logs.map((event) => all.indexOf(event)))
										}}
										onMouseLeave={() => {
											fullStep.current = false
										}}
									>
										<StepDisplay
											step={step}
											isHovered={isHovered}
											logs={logs}
											start={start}
											end={end}
											adjustDate={adjustDate}
											rtl={start > minDate + adjustedInterval * .75}
										/>
									</div>
								</SheetTrigger>
							</Fragment>
						)
					})}
				</Sheet>
				{longIntervals.map(([a], i) => {
					const start = adjustDate(a)
					const left = Math.max(0, (start - minDate) / adjustedInterval) * 100
					const end = start + adjustedLongEvent
					const width = (end - start) / adjustedInterval * 100
					return (
						<div
							key={i}
							className="absolute z-0 top-0 bottom-0 transition-all py-0"
							style={{
								left: `${left}%`,
								width: `${width}%`,
								paddingInline: `min(1rem, ${width / 4}%)`,
							}}
						>
							<div
								className="h-full text-stone-200 dark:text-stone-800"
								style={{
									backgroundImage: `repeating-linear-gradient(45deg, transparent, transparent 10px, currentColor 10px, currentColor 11px)`,
								}}
							/>
						</div>
					)
				})}
				{all.map((event, i) => {
					const time = adjustDate(event.created_at)
					return (
						<div
							key={i}
							className={clsx(
								"absolute top-0 bottom-0 transition-all pointer-events-none border-l",
								hoveredEvent.includes(i) ? 'z-20' : 'z-0',
								hoveredEvent.includes(i)
									? 'border-fuchsia-500 dark:border-fuchsia-400'
									: 'border-stone-200 dark:border-stone-800'
							)}
							style={{
								left: `min(calc(100% - 1px), ${Math.max(0, (time - minDate) / adjustedInterval) * 100 + '%'})`,
							}}
						/>
					)
				})}
			</div>
		</div>
	)
}


function StepDisplay({
	step,
	isHovered,
	logs,
	start,
	end,
	adjustDate,
	rtl,
}: {
	step: Step,
	isHovered: boolean,
	logs: SystemLog[],
	start: number,
	end: number,
	adjustDate: (date: number) => number,
	rtl: boolean,
}) {
	const bgs = []
	for (let i = 1; i < logs.length; i++) {
		const eventStart = adjustDate(logs[i - 1].created_at)
		const event = logs[i]!
		const eventEnd = adjustDate(event.created_at)
		const width = (eventEnd - eventStart) / (end - start) * 100
		const left = (eventStart - start) / (end - start) * 100
		const type = event.payload.event
		bgs.push(
			<div
				key={i}
				className={clsx(
					"absolute top-0 bottom-0 transition-all z-0",
					type === 'error' ? 'bg-red-500 dark:bg-red-800' : type === 'run' ? 'bg-stone-100 dark:bg-stone-900' : type === 'success' ? 'bg-emerald-600 dark:bg-emerald-800' : 'bg-stone-100 dark:bg-stone-900'
				)}
				style={{
					left: `${left}%`,
					width: `${width}%`,
				}}
			/>
		)
	}
	const isSleep = step.step.startsWith('system/sleep')
	const Icon = status[step.status]
	return (
		<div
			className={clsx(
				'z-0 transition-all',
				isHovered && 'text-fuchsia-500 dark:text-fuchsia-400',
				isSleep
					? 'bg-stone-100 dark:bg-stone-900'
					: step.status === 'completed' ? 'bg-emerald-600 dark:bg-emerald-800' : step.status === 'failed' ? 'bg-red-500 dark:bg-red-800' : 'bg-stone-100 dark:bg-stone-900'
			)}
			style={{
				height: 'calc(1lh + 1em)',
			}}
		>
			{bgs}
			<span
				className={clsx(
					"relative flex items-center z-10 whitespace-pre bg-stone-100/20 dark:bg-stone-900/20",
					rtl && 'justify-end'
				)}
				style={{ top: '0.5em', }}
			>
				{Icon}
				{` ${step.step} `}
			</span>
		</div>
	)
}

const status: Record<Step['status'], ReactElement> = {
	completed: <CircleCheckBig className="shrink-0 ml-1 h-4 w-4 text-emerald-500" />,
	failed: <CircleX className="shrink-0 ml-1 h-4 w-4 text-red-500" />,
	pending: <CircleDashed className="shrink-0 ml-1 h-4 w-4 text-stone-700 dark:text-stone-300" />,
	running: <Spin className="shrink-0 ml-1 h-4 w-4 text-amber-500" />,
	stalled: <Clock className="shrink-0 ml-1 h-4 w-4 text-cyan-700 dark:text-cyan-300" />,
	waiting: <Workflow className="shrink-0 ml-1 h-4 w-4 text-purple-500" />,
}

function Spin({ className }: { className?: string }) {
	return (
		<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" className={className}>
			<path d="M12,1A11,11,0,1,0,23,12,11,11,0,0,0,12,1Zm0,19a8,8,0,1,1,8-8A8,8,0,0,1,12,20Z" opacity=".25" fill="currentColor" />
			<path d="M10.14,1.16a11,11,0,0,0-9,8.92A1.59,1.59,0,0,0,2.46,12,1.52,1.52,0,0,0,4.11,10.7a8,8,0,0,1,6.66-6.61A1.42,1.42,0,0,0,12,2.69h0A1.57,1.57,0,0,0,10.14,1.16Z" fill="currentColor">
				<animateTransform attributeName="transform" type="rotate" dur="0.75s" values="0 12 12;360 12 12" repeatCount="indefinite" />
			</path>
		</svg>
	)
}



function StepDetails({ step, logs }: { step: Step, logs: Log[] }) {
	const { source: sheetSource, data: sheetOutput, ...sheetRest } = step
	return (
		<SheetContent className="min-w-[50%] w-fit overflow-y-auto">
			<SheetHeader>
				<SheetTitle>{step.step}</SheetTitle>
			</SheetHeader>
			<Tabs defaultValue="state" className="w-full my-4">
				<Card className="mt-4">
					<CardHeader>
						<TabsList>
							<TabsTrigger value="state">State</TabsTrigger>
							{sheetSource && (
								<TabsTrigger value="source">Source</TabsTrigger>
							)}
							{(sheetOutput) && (
								<TabsTrigger value="output">Output</TabsTrigger>
							)}
						</TabsList>
					</CardHeader>
					<CardContent>
						<TabsContent value="state">
							<Code language="json">
								{JSON.stringify(sheetRest, null, 2)}
							</Code>
						</TabsContent>
						{sheetSource && (
							<TabsContent value="source">
								<Code language="javascript" showLineNumbers>
									{sheetSource.trim()}
								</Code>
							</TabsContent>
						)}
						{(sheetOutput) && (
							<TabsContent value="output">
								{step.status === 'failed' ? (
									<ErrorDisplay error={sheetOutput} />
								) : (
									<Code language="json">
										{JSON.stringify(JSON.parse(sheetOutput), null, 2)}
									</Code>
								)}
							</TabsContent>
						)}
					</CardContent>
				</Card>
			</Tabs>

			{logs.map((event, i) => (
				<Fragment key={i}>
					{i > 0 && <Separator />}
					<LogDisplay log={event} />
				</Fragment>
			))}

		</SheetContent>
	)
}

function LogDisplay({ log }: { log: Log }) {
	const date = useFormatDate(log.created_at)
	const type = log.system ? log.payload.event : 'log'
	return (
		<div className="my-8">
			<p className="text-sm">{date}</p>
			<p className="text-xl my-1">{type}</p>
			{log.system && log.payload.event === 'error' && (
				<ErrorDisplay error={log.payload.error} />
			)}
			{log.system && log.payload.event !== 'error' && (
				<Code language="json">
					{JSON.stringify(log.payload, null, 2)}
				</Code>
			)}
			{!log.system && typeof log.payload === 'string' && (
				<>
					<p className="text-stone-500">{log.payload}</p>
				</>
			)}
			{!log.system && typeof log.payload !== 'string' && (
				<>
					<Code language="json">
						{JSON.stringify(log.payload, null, 2)}
					</Code>
				</>
			)}
		</div>
	)
}

function ErrorDisplay({ error, indent = '' }: { error: string, indent?: string }) {
	const obj = JSON.parse(error) as { name: string, message: string, stack: string, cause?: string }
	return (
		<pre className="whitespace-pre-wrap">
			<span>{indent}<span className="font-bold">{obj.name}</span>: <span className="italic">{obj.message}</span></span>
			{obj.stack.split('\n').map((l, i) => (
				<span className="text-stone-500" key={i}>{`\n${indent}`}{l}</span>
			))}
			{obj.cause && (<>
				{`\n${indent}  `}
				<ErrorDisplay error={obj.cause} indent={`${indent}  `} />
			</>)}
		</pre>
	)
}
