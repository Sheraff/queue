import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { Job } from "./Job"
import { Button } from "client/components/ui/button"


function App() {
  const [job, setJob] = useState<string | null>(null)

  const { data } = useQuery({
    queryKey: ['jobs'],
    queryFn: async () => {
      const res = await fetch('/api/jobs')
      const json = await res.json()
      return json as string[]
    }
  })

  return (
    <>
      <nav>
        <ul className="flex gap-1">
          {data?.map((job) => (
            <li key={job}>
              <Button type="button" onClick={() => setJob(j => j === job ? null : job)}>{job}</Button>
            </li>
          ))}
        </ul>
      </nav>
      {job && <Job job={job} />}
    </>
  )
}

export default App
