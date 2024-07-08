import { useQuery, useQueryClient } from "@tanstack/react-query"
import { createContext, useContext, useEffect, type ReactNode } from "react"

const InnerNowContext = createContext<number | undefined>(undefined)

export function useNow() {
	const now = useContext(InnerNowContext)
	if (now === undefined) {
		return Date.now() / 1000
	}
	return now
}

export function NowContext({ children }: { children: ReactNode }) {
	const { data, refetch } = useQuery({
		queryKey: ['now'],
		queryFn: async () => {
			const res = await fetch('/api/now')
			const now = Number(await res.text())
			const delta = (Date.now() / 1000) - now
			return [now, delta] as const
		},
		staleTime: Infinity,
		gcTime: Infinity,
	})

	const client = useQueryClient()

	const delta = data?.[1]

	useEffect(() => {
		if (delta === undefined) return
		let i = 1
		const interval = setInterval(() => {
			i = (i + 1) % 120
			client.setQueryData(['now'], [(Date.now() / 1000) - delta, delta])
			if (i === 0) refetch()
		}, 1000)
		return () => clearInterval(interval)
	}, [delta, client, refetch])

	return <InnerNowContext.Provider value={data?.[0]}>{children}</InnerNowContext.Provider>
}