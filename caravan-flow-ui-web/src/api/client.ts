import type {
  Flow,
  ProcessorStatsMap,
  ProvenanceEvent,
  RegistryEntry,
  SourceInfo,
  VcStatus,
} from './types'

// Same-origin fetch wrappers for the management API. In dev, Vite's
// proxy forwards /api/* and /metrics to the worker (see vite.config).
// In prod, the worker hosts this bundle on its own port so every path
// is same-origin anyway.

async function getJson<T>(url: string): Promise<T> {
  const r = await fetch(url)
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${url}`)
  return (await r.json()) as T
}

async function getText(url: string): Promise<string> {
  const r = await fetch(url)
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}: ${url}`)
  return await r.text()
}

async function send(
  method: string,
  url: string,
  body?: unknown,
): Promise<Response> {
  return fetch(url, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
}

export const api = {
  // --- runtime ---
  flow: () => getJson<Flow>('/api/flow'),
  processorStats: () => getJson<ProcessorStatsMap>('/api/processor-stats'),
  registry: () => getJson<RegistryEntry[]>('/api/registry'),

  // --- processor crud ---
  addProcessor: (body: {
    name: string
    type: string
    config?: Record<string, unknown>
    connections?: Record<string, string[]>
  }) => send('POST', '/api/processors/add', body),
  removeProcessor: (name: string) =>
    send('DELETE', `/api/processors/remove?name=${encodeURIComponent(name)}`),
  enableProcessor: (name: string) => send('POST', '/api/processors/enable', { name }),
  disableProcessor: (name: string) => send('POST', '/api/processors/disable', { name }),
  updateProcessorConfig: (
    name: string,
    body: { type?: string; config: Record<string, unknown> },
  ) => send('PUT', `/api/processors/${encodeURIComponent(name)}/config`, body),

  // --- connections ---
  addConnection: (from: string, relationship: string, to: string) =>
    send('POST', '/api/connections', { from, relationship, to }),
  removeConnection: (from: string, relationship: string, to: string) =>
    send('DELETE', '/api/connections', { from, relationship, to }),
  setConnections: (from: string, relationships: Record<string, string[]>) =>
    send('PUT', `/api/connections/${encodeURIComponent(from)}`, relationships),

  // --- entry points ---
  setEntryPoints: (names: string[]) => send('PUT', '/api/entrypoints', { names }),

  // --- providers ---
  updateProviderConfig: (name: string, config: Record<string, unknown>) =>
    send('PUT', `/api/providers/${encodeURIComponent(name)}/config`, config),

  // --- provenance ---
  provenanceRecent: (n = 100) => getJson<ProvenanceEvent[]>(`/api/provenance?n=${n}`),
  provenanceById: (id: string) => getJson<ProvenanceEvent[]>(`/api/provenance/${id}`),
  provenanceFailures: (n = 50) =>
    getJson<ProvenanceEvent[]>(`/api/provenance/failures?n=${n}`),

  // --- overlays / VC ---
  overlays: () => getJson<unknown>('/api/overlays'),
  vcStatus: () => getJson<VcStatus>('/api/vc/status'),

  // --- flow save / reload ---
  // saveFlow defaults to "write disk + commit + push" (the explicit dev
  // action). Auto-save callers pass {commit: false} to skip VC.
  saveFlow: (body: { message?: string; push?: boolean; commit?: boolean } = {}) =>
    send('POST', '/api/flow/save', {
      message: body.message ?? 'flow: update via UI',
      push: body.push ?? true,
      commit: body.commit ?? true,
    }),
  flowStatus: () => getJson<{
    dirty: boolean
    mutationCounter: number
    lastSavedCounter: number
    lastSavedAgoMs: number | null
  }>('/api/flow/status'),
  reloadFlow: () => send('POST', '/api/reload'),

  // --- Layout sibling (team-shared node positions) ---
  layout: () => getJson<{ positions: Record<string, { x: number; y: number }>; path?: string }>('/api/layout'),
  saveLayout: (positions: Record<string, { x: number; y: number }>) =>
    send('POST', '/api/layout', { positions }),

  // --- sources ---
  sources: () => getJson<SourceInfo[]>('/api/sources'),
  startSource: (name: string) => send('POST', '/api/sources/start', { name }),
  stopSource: (name: string) => send('POST', '/api/sources/stop', { name }),
  addSource: (body: { name: string; type: string; config?: Record<string, unknown> }) =>
    send('POST', '/api/sources/add', body),

  // --- per-processor ---
  resetProcessorStats: (name: string) =>
    send('POST', `/api/processors/${encodeURIComponent(name)}/stats/reset`),

  // --- test ingest ---
  ingestFlowFile: (body: {
    target: string
    content?: string
    contentBase64?: string
    attributes?: Record<string, string>
  }) => send('POST', '/api/flowfiles/ingest', body),

  // --- metrics ---
  metrics: () => getText('/metrics'),
}
