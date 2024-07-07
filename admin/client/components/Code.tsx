import { useTheme } from "client/components/theme-provider"
import type { CSSProperties } from "react"
import { LightAsync } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import json from 'react-syntax-highlighter/dist/esm/languages/hljs/json'

LightAsync.registerLanguage('javascript', js)
LightAsync.registerLanguage('json', json)

const light = {
	"hljs-comment": {
		"color": "#8e908c"
	},
	"hljs-quote": {
		"color": "#8e908c"
	},
	"hljs-variable": {
		"color": "#c82829"
	},
	"hljs-template-variable": {
		"color": "#c82829"
	},
	"hljs-tag": {
		"color": "#c82829"
	},
	"hljs-name": {
		"color": "#c82829"
	},
	"hljs-selector-id": {
		"color": "#c82829"
	},
	"hljs-selector-class": {
		"color": "#c82829"
	},
	"hljs-regexp": {
		"color": "#c82829"
	},
	"hljs-deletion": {
		"color": "#c82829"
	},
	"hljs-number": {
		"color": "#f5871f"
	},
	"hljs-built_in": {
		"color": "#f5871f"
	},
	"hljs-builtin-name": {
		"color": "#f5871f"
	},
	"hljs-literal": {
		"color": "#f5871f"
	},
	"hljs-type": {
		"color": "#f5871f"
	},
	"hljs-params": {
		"color": "#f5871f"
	},
	"hljs-meta": {
		"color": "#f5871f"
	},
	"hljs-link": {
		"color": "#f5871f"
	},
	"hljs-attribute": {
		"color": "#eab700"
	},
	"hljs-string": {
		"color": "#718c00"
	},
	"hljs-symbol": {
		"color": "#718c00"
	},
	"hljs-bullet": {
		"color": "#718c00"
	},
	"hljs-addition": {
		"color": "#718c00"
	},
	"hljs-title": {
		"color": "#4271ae"
	},
	"hljs-section": {
		"color": "#4271ae"
	},
	"hljs-keyword": {
		"color": "#8959a8"
	},
	"hljs-selector-tag": {
		"color": "#8959a8"
	},
	"hljs": {
		"display": "block",
		"overflowX": "auto",
		"color": "#4d4d4c",
		"padding": "0.5em"
	},
	"hljs-emphasis": {
		"fontStyle": "italic"
	},
	"hljs-strong": {
		"fontWeight": "bold"
	}
} satisfies { [key: string]: CSSProperties }
const dark = {
	"hljs": {
		"display": "block",
		"overflowX": "auto",
		"padding": "0.5em",
		"color": "#d6deeb"
	},
	"hljs-keyword": {
		"color": "#c792ea",
		"fontStyle": "italic"
	},
	"hljs-built_in": {
		"color": "#addb67",
		"fontStyle": "italic"
	},
	"hljs-type": {
		"color": "#82aaff"
	},
	"hljs-literal": {
		"color": "#ff5874"
	},
	"hljs-number": {
		"color": "#F78C6C"
	},
	"hljs-regexp": {
		"color": "#5ca7e4"
	},
	"hljs-string": {
		"color": "#ecc48d"
	},
	"hljs-subst": {
		"color": "#d3423e"
	},
	"hljs-symbol": {
		"color": "#82aaff"
	},
	"hljs-class": {
		"color": "#ffcb8b"
	},
	"hljs-function": {
		"color": "#82AAFF"
	},
	"hljs-title": {
		"color": "#DCDCAA",
		"fontStyle": "italic"
	},
	"hljs-params": {
		"color": "#7fdbca"
	},
	"hljs-comment": {
		"color": "#637777",
		"fontStyle": "italic"
	},
	"hljs-doctag": {
		"color": "#7fdbca"
	},
	"hljs-meta": {
		"color": "#82aaff"
	},
	"hljs-meta-keyword": {
		"color": "#82aaff"
	},
	"hljs-meta-string": {
		"color": "#ecc48d"
	},
	"hljs-section": {
		"color": "#82b1ff"
	},
	"hljs-tag": {
		"color": "#7fdbca"
	},
	"hljs-name": {
		"color": "#7fdbca"
	},
	"hljs-builtin-name": {
		"color": "#7fdbca"
	},
	"hljs-attr": {
		"color": "#7fdbca"
	},
	"hljs-attribute": {
		"color": "#80cbc4"
	},
	"hljs-variable": {
		"color": "#addb67"
	},
	"hljs-bullet": {
		"color": "#d9f5dd"
	},
	"hljs-code": {
		"color": "#80CBC4"
	},
	"hljs-emphasis": {
		"color": "#c792ea",
		"fontStyle": "italic"
	},
	"hljs-strong": {
		"color": "#addb67",
		"fontWeight": "bold"
	},
	"hljs-formula": {
		"color": "#c792ea"
	},
	"hljs-link": {
		"color": "#ff869a"
	},
	"hljs-quote": {
		"color": "#697098",
		"fontStyle": "italic"
	},
	"hljs-selector-tag": {
		"color": "#ff6363"
	},
	"hljs-selector-id": {
		"color": "#fad430"
	},
	"hljs-selector-class": {
		"color": "#addb67",
		"fontStyle": "italic"
	},
	"hljs-selector-attr": {
		"color": "#c792ea",
		"fontStyle": "italic"
	},
	"hljs-selector-pseudo": {
		"color": "#c792ea",
		"fontStyle": "italic"
	},
	"hljs-template-tag": {
		"color": "#c792ea"
	},
	"hljs-template-variable": {
		"color": "#addb67"
	},
	"hljs-addition": {
		"color": "#addb67ff",
		"fontStyle": "italic"
	},
	"hljs-deletion": {
		"color": "#EF535090",
		"fontStyle": "italic"
	}
} satisfies { [key: string]: CSSProperties }

export function Code({ children, language = 'javascript', className }: { children: string, language?: 'javascript' | 'json', className?: string }) {
	const { theme } = useTheme()
	return <LightAsync language={language} style={theme === 'dark' ? dark : light} className={className}>
		{children}
	</LightAsync>
}