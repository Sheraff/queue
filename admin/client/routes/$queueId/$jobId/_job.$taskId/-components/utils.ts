export const cleanEventName = (name: string, job: { job: string }) => name
	.replace(new RegExp(`^job\\/${job.job}\\/`), '')
	.replace(new RegExp(`^step\\/${job.job}\\/`), '')