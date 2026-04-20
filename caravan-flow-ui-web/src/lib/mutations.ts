import { useMutation, useQueryClient, type UseMutationOptions } from '@tanstack/react-query'
import { api } from '../api/client'

// Every mutation invalidates the flow and stats queries on success so
// the graph + drawer reconcile instantly — no 15 s wait for the
// topology poll after a Save.

function useFlowMutation<TVars, TData>(
  mutationFn: (vars: TVars) => Promise<TData>,
  opts?: Omit<UseMutationOptions<TData, Error, TVars>, 'mutationFn'>,
) {
  const qc = useQueryClient()
  return useMutation<TData, Error, TVars>({
    mutationFn,
    ...opts,
    onSuccess: async (...args) => {
      await qc.invalidateQueries({ queryKey: ['flow'] })
      await qc.invalidateQueries({ queryKey: ['processor-stats'] })
      await qc.invalidateQueries({ queryKey: ['vc-status'] })
      opts?.onSuccess?.(...args)
    },
  })
}

async function unwrap(resp: Response): Promise<Response> {
  if (!resp.ok) {
    const text = await resp.text().catch(() => '')
    throw new Error(`${resp.status} ${resp.statusText}${text ? `: ${text}` : ''}`)
  }
  return resp
}

export function useUpdateProcessorConfig() {
  return useFlowMutation<
    { name: string; type?: string; config: Record<string, unknown> },
    Response
  >(async ({ name, type, config }) => unwrap(await api.updateProcessorConfig(name, { type, config })))
}

export function useSetConnections() {
  return useFlowMutation<{ from: string; relationships: Record<string, string[]> }, Response>(
    async ({ from, relationships }) => unwrap(await api.setConnections(from, relationships)),
  )
}

export function useAddConnection() {
  return useFlowMutation<{ from: string; relationship: string; to: string }, Response>(
    async ({ from, relationship, to }) => unwrap(await api.addConnection(from, relationship, to)),
  )
}

export function useRemoveConnection() {
  return useFlowMutation<{ from: string; relationship: string; to: string }, Response>(
    async ({ from, relationship, to }) => unwrap(await api.removeConnection(from, relationship, to)),
  )
}

export function useToggleProcessor() {
  return useFlowMutation<{ name: string; enabled: boolean }, Response>(
    async ({ name, enabled }) =>
      unwrap(await (enabled ? api.enableProcessor(name) : api.disableProcessor(name))),
  )
}

export function useRemoveProcessor() {
  return useFlowMutation<string, Response>(async (name) => unwrap(await api.removeProcessor(name)))
}

export function useAddProcessor() {
  return useFlowMutation<
    { name: string; type: string; config?: Record<string, unknown>; connections?: Record<string, string[]> },
    Response
  >(async (body) => unwrap(await api.addProcessor(body)))
}

export function useSetEntryPoints() {
  return useFlowMutation<string[], Response>(async (names) => unwrap(await api.setEntryPoints(names)))
}

export function useSaveFlow() {
  return useFlowMutation<{ message?: string; push?: boolean }, Record<string, unknown>>(
    async (body) => {
      const resp = await unwrap(await api.saveFlow(body))
      return (await resp.json()) as Record<string, unknown>
    },
  )
}

export function useReloadFlow() {
  return useFlowMutation<void, Record<string, unknown>>(async () => {
    const resp = await unwrap(await api.reloadFlow())
    return (await resp.json()) as Record<string, unknown>
  })
}

export function useAddSource() {
  return useFlowMutation<
    { name: string; type: string; config?: Record<string, unknown> },
    Response
  >(async (body) => unwrap(await api.addSource(body)))
}

export function useStartSource() {
  return useFlowMutation<string, Response>(async (name) => unwrap(await api.startSource(name)))
}

export function useStopSource() {
  return useFlowMutation<string, Response>(async (name) => unwrap(await api.stopSource(name)))
}

export function useResetProcessorStats() {
  return useFlowMutation<string, Response>(async (name) =>
    unwrap(await api.resetProcessorStats(name)),
  )
}

export function useIngestFlowFile() {
  return useFlowMutation<
    { target: string; content?: string; contentBase64?: string; attributes?: Record<string, string> },
    Record<string, unknown>
  >(async (body) => {
    const resp = await unwrap(await api.ingestFlowFile(body))
    return (await resp.json()) as Record<string, unknown>
  })
}
