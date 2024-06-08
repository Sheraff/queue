import { aaa } from "./programs/aaa.js"
import { pokemon } from "./programs/pokemon.js"

import { db } from "./db/instance.js"


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


const programs = new Map<string, (ctx: Ctx) => void>()

function registerProgram(name: string, program: (ctx: Ctx<any>) => void) {
	programs.set(name, program)
}

registerProgram(aaa.name, aaa)
registerProgram(pokemon.name, pokemon)


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
function registerTask(id: string, program: string, initialData: Data = {}) {
	if (!programs.has(program)) throw new Error(`Unknown program: ${program}. Available programs: ${[...programs.keys()].join(', ')}`)
	register.run({ id, program, data: JSON.stringify(initialData) })
}
// ids should be static for idempotency, but for now we'll just generate random ids
registerTask(crypto.randomUUID(), 'aaa', { a: 1 })
registerTask(crypto.randomUUID(), 'pokemon', { id: 2 })
registerTask(crypto.randomUUID(), 'aaa', { a: 2 })
registerTask(crypto.randomUUID(), 'aaa', { a: 3 })
registerTask(crypto.randomUUID(), 'pokemon', { id: 151 })


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
do {
	const task = getFirstTask.get()
	if (!task) break
	console.log('handle', task.id, task.program, task.step)
	await handleProgram(task, programs.get(task.program)!)
} while (true)


