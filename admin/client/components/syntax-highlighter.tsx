import { LightAsync } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import json from 'react-syntax-highlighter/dist/esm/languages/hljs/json'

LightAsync.registerLanguage('javascript', js)
LightAsync.registerLanguage('json', json)

export function Code({ children, language = 'javascript', className }: { children: string, language?: 'javascript' | 'json', className?: string }) {
	return <LightAsync language={language} style={{}} className={className} useInlineStyles={false}>
		{children}
	</LightAsync>
}