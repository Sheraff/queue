import Database from 'better-sqlite3'
import { readFileSync } from "fs"

const db = new Database('foobar.db', {})
db.pragma('journal_mode = WAL')
db.exec(readFileSync(new URL('./schema.sql', import.meta.url), 'utf8'))


const close = () => {
	db.close()
}
process.on('exit', close)
process.on('SIGINT', () => {
	close()
	process.exit(0)
})
// process.on('uncaughtException', close)
// process.on('SIGTERM', close)
// process.on('unhandledRejection', close)

export { db }