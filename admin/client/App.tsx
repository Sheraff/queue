import { useQuery } from "@tanstack/react-query"
import { useMemo, useState } from "react"
import { Job } from "./jobs/Job"
import { Button } from "client/components/ui/button"
import { ModeToggle } from "client/components/mode-toggle"
import { NowContext } from "client/now"
import { TimeDisplayToggle } from "client/components/time-display-toggle"


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
    <NowContext >
      <nav className="flex p-2 gap-2">
        <ul className="flex gap-1">
          {data?.map((job) => (
            <li key={job}>
              <Button type="button" onClick={() => setJob(j => j === job ? null : job)}>{job}</Button>
            </li>
          ))}
        </ul>
        <div className="flex-1" />
        <TimeDisplayToggle />
        <ModeToggle />
      </nav>
      {useMemo(() => job && <Job job={job} />, [job])}
    </NowContext>
  )
}

export default App
