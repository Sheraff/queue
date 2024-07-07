import { createContext, useContext, useEffect, useRef, useState } from "react"

type Theme = "dark" | "light"

type ThemeProviderProps = {
	children: React.ReactNode
	defaultTheme?: Theme | "system"
	storageKey?: string
}

type ThemeProviderState = {
	theme: "dark" | "light"
	setTheme: (theme: Theme | 'system') => void
}

const ThemeProviderContext = createContext<ThemeProviderState | null>(null)

const getSystem = (): Theme => window.matchMedia("(prefers-color-scheme: dark)")
	.matches
	? "dark"
	: "light"

export function ThemeProvider({
	children,
	defaultTheme = "system",
	storageKey = "vite-ui-theme",
	...props
}: ThemeProviderProps) {
	const [theme, setTheme] = useState<Theme>(
		() => {
			const theme = (localStorage.getItem(storageKey) ?? defaultTheme) as Theme | "system"
			if (theme === "system") return getSystem()
			return theme
		}
	)
	const foo = useRef(false)
	if (!foo.current) {
		foo.current = true
		const root = window.document.documentElement
		root.classList.remove("light", "dark")
		root.classList.add(theme)
	}

	useEffect(() => {
		const root = window.document.documentElement
		root.classList.remove("light", "dark")
		root.classList.add(theme)
		// TODO: mediaQuery.addListener
	}, [theme])

	const value = {
		theme,
		setTheme: (theme: Theme | "system") => {
			localStorage.setItem(storageKey, theme)
			const t = theme === "system" ? getSystem() : theme
			setTheme(t)
		},
	}

	return (
		<ThemeProviderContext.Provider {...props} value={value}>
			{children}
		</ThemeProviderContext.Provider>
	)
}

export const useTheme = () => {
	const context = useContext(ThemeProviderContext)

	if (!context)
		throw new Error("useTheme must be used within a ThemeProvider")

	return context
}
