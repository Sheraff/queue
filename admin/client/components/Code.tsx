import { LightAsync } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import json from 'react-syntax-highlighter/dist/esm/languages/hljs/json'
import docco from 'react-syntax-highlighter/dist/esm/styles/hljs/docco'

LightAsync.registerLanguage('javascript', js)
LightAsync.registerLanguage('json', json)

export function Code({ children, language = 'javascript' }: { children: string, language?: 'javascript' | 'json' }) {
	return <LightAsync language={language} style={docco}>
		{children}
	</LightAsync>
}