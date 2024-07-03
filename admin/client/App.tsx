import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { Job } from "./Job"

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
        <ul>
          {data?.map((job) => (
            <li key={job}>
              <button type="button" onClick={() => setJob(job)}>{job}</button>
            </li>
          ))}
        </ul>
      </nav>
      {job && <Job job={job} />}
    </>
  )
}

export default App
