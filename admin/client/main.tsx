import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import './global.css'
import { ThemeProvider } from "client/components/theme-provider"

const client = new QueryClient({
  defaultOptions: {
    queries: {
      gcTime: 15 * 60 * 1000, // 15 minutes
      staleTime: 10 * 60 * 1000, // 10 minutes
    }
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <App />
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>,
)
