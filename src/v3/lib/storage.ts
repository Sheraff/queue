export interface Storage {
	close(): void | Promise<void>
}


export class SQLiteStorage implements Storage {
	constructor() {
	}
	close() { }
}