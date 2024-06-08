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
	status: 'pending' | 'running' | 'success' | 'failure'
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
	status: Task['status']
}, Task>(/* sql */`
	UPDATE tasks
	SET data = @data, step = @step, status = @status, updated_at = CURRENT_TIMESTAMP
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
		try {
			const augment = await asyncLocalStorage.run(task, () => next[1](data))
			Object.assign(data, augment)
		} catch (e) {
			console.error(e)
			storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step, status: 'failure' })
			return
		}
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
			const result = storeTask.get({ id: params.id, data: JSON.stringify(params.data), step: task.step, status: 'success' })
			if (result?.parent_id && result?.parent_key) {
				const parent = getTask.get({ id: result.parent_id })
				if (!parent) throw new Error('Parent not found')
				const parentData = JSON.parse(parent.data) as Data
				parentData[result.parent_key] = params.data
				storeTask.run({ id: parent.id, data: JSON.stringify(parentData), step: parent.step, status: parent.status })
			}
		})
		tx({ id: task.id, data })
	} else {
		storeTask.run({ id: task.id, data: JSON.stringify(data), step: task.step, status: 'pending' })
	}
}

/**
 * select any task
 * - that is not referenced by any other task under the `parent_id` column
 */
const getFirstTask = db.prepare<[], Task>(/* sql */`
	SELECT * FROM tasks
	WHERE
		id NOT IN (
			SELECT parent_id FROM tasks
			WHERE parent_id IS NOT NULL
				AND status NOT IN ('success', 'failure')
		)
		AND status = 'pending'
	ORDER BY created_at ASC
	LIMIT 1
`)
const getTaskCount = db.prepare<[], { count: number }>(/* sql */`
	SELECT COUNT(*) as count FROM tasks
	WHERE status NOT IN ('success', 'failure')
`)
const getNext = db.transaction(() => {
	const task = getFirstTask.get()
	if (task) {
		storeTask.run({ id: task.id, data: task.data, step: task.step, status: 'running' })
	}
	return task
})
export async function handleNext() {
	const task = getNext()
	if (task) {
		console.log('handle', task.id, task.program, task.step)
		handleProgram(task, programs.get(task.program)!)
		return "next"
	}
	const count = getTaskCount.get()
	if (count?.count) {
		return "wait"
	}
	return "done"
}
