import { createLocalStorageHook } from "client/components/use-local-storage"

type TimeDisplayState = 'relative' | 'absolute'

export const useTimeDisplay = createLocalStorageHook<TimeDisplayState>('time-display', 'relative')