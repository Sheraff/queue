import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import './global.css'
import { ThemeProvider } from "client/components/theme-provider"
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { routeTree } from './routeTree.gen'
import { NowContext } from "client/now"

const client = new QueryClient({
  defaultOptions: {
    queries: {
      gcTime: 15 * 60 * 1000, // 15 minutes
      staleTime: 10 * 60 * 1000, // 10 minutes
    }
  },
})

const router = createRouter({
  routeTree,
  context: {
    client,
  },
})
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <NowContext>
          <RouterProvider router={router} />
        </NowContext>
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>,
)
