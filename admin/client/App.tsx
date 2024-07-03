import { useQuery } from "@tanstack/react-query"

function App() {
  const { data } = useQuery({
    queryKey: ['foo'],
    queryFn: async () => {
      const res = await fetch('/api')
      const json = await res.json()
      return json
    }
  })

  return (
    <>
      <pre>
        {JSON.stringify(data, null, '\t')}
      </pre>
    </>
  )
}

export default App
