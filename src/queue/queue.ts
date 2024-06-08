import { db } from "../db/instance.js"

type GenericSerializable = string | number | boolean | null | GenericSerializable[] | { [key: string]: GenericSerializable }

const dataKey = Symbol('data')
type dataKey = typeof dataKey
export type Data = { [key: string]: GenericSerializable }

export interface Ctx<InitialData extends Data = {}> {
	[dataKey]: Data & InitialData
	step<T extends Partial<this[dataKey]>>(cb: (d: this[dataKey]) => (Promise<T> | Promise<void> | T | void)): asserts this is { [dataKey]: T }
	sleep(ms: number): void
}

type Task = {
	/** uuid */
	id: string
	program: string
	/** json */
	data: string
	step: number
	/** datetime */
	created_at: string
	/** datetime */
	updated_at: string
}

/*************************************************/
/* REGISTER ALL PROGRAMS AT THE START OF THE APP */
/*************************************************/

declare global {
	interface Program {
		/**
		 * this is to be augmented by each program
		 */
	}
}

const programs = new Map<string, (ctx: Ctx<any>) => void>()

export function registerProgram<P extends keyof Program>(name: P, program: (ctx: Ctx<Program[P]>) => void) {
	programs.set(name, program)
}

/********************************************/
/* REGISTER TASKS DURING THE APP'S LIFETIME */
/********************************************/

const register = db.prepare<{
	id: string
	program: string
	data: string
}>(/* sql */`
	INSERT INTO tasks (id, program, data)
	VALUES (@id, @program, @data)
`)
export function registerTask<P extends keyof Program>(id: string, program: P, initialData: object extends Program[P] ? (object | void) : Program[P]) {
	if (!programs.has(program)) throw new Error(`Unknown program: ${program}. Available programs: ${[...programs.keys()].join(', ')}`)
	register.run({ id, program, data: JSON.stringify(initialData) })
}

/***************************************/
/* DEPILE TASKS WITH A REOCCURRING JOB */
/***************************************/


const storeTask = db.prepare<{
	id: string
	data: string
	step: number
}>(/* sql */`
	UPDATE tasks
	SET data = @data, step = @step, updated_at = CURRENT_TIMESTAMP
	WHERE id = @id
`)
const deleteTask = db.prepare<{
	id: string
}>(/* sql */`
	DELETE FROM tasks
	WHERE id = @id
`)
async function handleProgram(task: Task, program: (ctx: Ctx) => void) {
	const steps: Array<
		| ['callback', (data: Data) => Promise<Data | void>]
		| ['sleep', number]
	> = []
	const ctx: Ctx = {
		[dataKey]: new Proxy({}, {
			get() {
				throw new Error('Only access data from inside a `step` callback, it is available as the first argument: `ctx.step(data => { data.yourKey })`')
			}
		}),
		step(cb) {
			steps.push(['callback', cb as any])
		},
		sleep(ms) {
			steps.push(['sleep', ms])
		},
	}
	program(ctx)

	// run next step
	const next = steps[task.step]
	if (!next) {
		throw new Error('No next step found')
	}

	const data = JSON.parse(task.data) as Data

	if (next[0] === 'callback') {
		Object.assign(data, await next[1](data))
	} else if (next[0] === 'sleep') {
		// not implemented
	} else {
		throw new Error('Unknown step type')
	}
	task.step++

	// delete task from DB
	if (task.step === steps.length) {
		console.log('task done', task.id, data)
		deleteTask.run({ id: task.id })
	} else {
		storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step })
	}
}
const getFirstTask = db.prepare<[], Task>(/* sql */`
	SELECT * FROM tasks
	ORDER BY id
	LIMIT 1
`)
export async function handleNext(): Promise<boolean> {
	const task = getFirstTask.get()
	if (!task) return false
	console.log('handle', task.id, task.program, task.step)
	await handleProgram(task, programs.get(task.program)!)
	return true
}