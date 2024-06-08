import { AsyncLocalStorage } from "async_hooks"
import { db } from "../db/instance.js"

type GenericSerializable = string | number | boolean | null | GenericSerializable[] | { [key: string]: GenericSerializable }

export type Data = { [key: string]: GenericSerializable }

export interface Ctx<InitialData extends Data = {}> {
	data: Data & InitialData
	step<T extends Partial<this["data"]>>(
		cb: (
			data: this["data"],
		) => (Promise<T> | Promise<void> | T | void)
	): asserts this is { data: T }
	sleep(ms: number): void
	registerTask<
		P extends keyof Program,
		K extends string,
		Cond extends undefined | ((data: this["data"]) => boolean)
	>(
		program: P,
		initialData: Program[P]['initial'],
		key: K,
		condition?: Cond
	): asserts this is {
		data: { [key in K]?: Program[P]['result'] }
	}
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
	/** uuid */
	parent_id: string | null
	parent_key: string | null
}

/*************************************************/
/* REGISTER ALL PROGRAMS AT THE START OF THE APP */
/*************************************************/

type BaseProgram = Record<string, {
	initial: Data
	result: Data
	children?: { [key: string]: Data }
}>

declare global {
	interface Program extends BaseProgram {
		/**
		 * this is to be augmented by each program
		 */
	}
}

const programs = new Map<keyof Program, (ctx: Ctx<any>) => (void | Promise<void>)>()

export function registerProgram<P extends keyof Program>(name: P, program: (ctx: Ctx<Program[P]['initial']>) => (void | Promise<void>)) {
	programs.set(name, program)
}

/********************************************/
/* REGISTER TASKS DURING THE APP'S LIFETIME */
/********************************************/

const asyncLocalStorage = new AsyncLocalStorage<Task>()

const register = db.prepare<{
	id: string
	program: keyof Program
	data: string
	parent: string | null
	parent_key: string | null
}>(/* sql */`
	INSERT INTO tasks (id, program, data, parent_id, parent_key)
	VALUES (@id, @program, @data, @parent, @parent_key)
`)
export function registerTask<P extends keyof Program>(id: string, program: P, initialData: Program[P]['initial'], parentKey?: string) {
	if (!programs.has(program)) throw new Error(`Unknown program: ${program}. Available programs: ${[...programs.keys()].join(', ')}`)
	const parent = asyncLocalStorage.getStore()
	console.log('register', program, parent?.id, parentKey, id)
	register.run({ id, program, data: JSON.stringify(initialData), parent: parent?.id ?? null, parent_key: parentKey ?? null })
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
}, Task>(/* sql */`
	DELETE FROM tasks
	WHERE id = @id
	RETURNING *
`)
const getTask = db.prepare<{
	id: string
}, Task>(/* sql */`
	SELECT * FROM tasks
	WHERE id = @id
`)
async function handleProgram(task: Task, program: (ctx: Ctx) => (void | Promise<void>)) {
	const data = JSON.parse(task.data) as Data

	const steps: Array<
		| ['callback', (data: Data) => Promise<Data | void>]
		| ['sleep', duration: number]
		| ['register', program: keyof Program, initial: Data, key: string, condition?: (data: Data) => boolean]
	> = []
	const ctx: Ctx = {
		data,
		step(cb) {
			steps.push(['callback', cb as any])
		},
		sleep(ms) {
			steps.push(['sleep', ms])
		},
		registerTask(program, initialData, key, condition) {
			steps.push(['register', program, initialData, key, condition])
		},
	}
	await program(ctx)

	// run next step
	const next = steps[task.step]
	if (!next) {
		throw new Error('No next step found')
	}

	if (next[0] === 'callback') {
		const augment = await asyncLocalStorage.run(task, () => next[1](data))
		Object.assign(data, augment)
	} else if (next[0] === 'sleep') {
		// not implemented
	} else if (next[0] === 'register') {
		const [_, program, initial, key, condition] = next
		if (!condition || condition(data)) {
			asyncLocalStorage.run(task, () => registerTask(crypto.randomUUID(), program, initial, key))
		}
	} else {
		throw new Error('Unknown step type')
	}
	task.step++

	// delete task from DB
	if (task.step === steps.length) {
		console.log('task done', task.id, data)
		const tx = db.transaction((params: { id: string, data: Data }) => {
			const result = deleteTask.get({ id: params.id })
			if (result?.parent_id && result?.parent_key) {
				const parent = getTask.get({ id: result.parent_id })
				if (!parent) throw new Error('Parent not found')
				const parentData = JSON.parse(parent.data) as Data
				parentData[result.parent_key] = params.data
				storeTask.run({ id: parent.id, data: JSON.stringify(parentData), step: parent.step })
			}
		})
		tx({ id: task.id, data })
	} else {
		storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step })
	}
}

/**
 * select any task
 * - that is not referenced by any other task under the `parent_id` column
 */
const getFirstTask = db.prepare<[], Task>(/* sql */`
	SELECT * FROM tasks
	WHERE id NOT IN (SELECT parent_id FROM tasks WHERE parent_id IS NOT NULL)
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
