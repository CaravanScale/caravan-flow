import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { Processor, RegistryEntry } from '../api/types'
import {
  useRemoveProcessor,
  useRemoveSource,
  useResetProcessorStats,
  useSetConnections,
  useToggleProcessor,
  useUpdateProcessorConfig,
  useUpdateSourceConfig,
} from '../lib/mutations'
import { ProcessorConfigForm } from './ProcessorConfigForm'
import { ConfirmDialog } from './ConfirmDialog'

type Tab = 'config' | 'connections' | 'stats' | 'sample'

interface Props {
  processor: Processor
  allProcessorNames: string[]
  isSource?: boolean
  onClose: () => void
}

interface ConfigRow { key: string; value: string }
interface ConnRow { rel: string; target: string }

function toConfigRows(p: Processor): ConfigRow[] {
  if (!p.config) return []
  return Object.entries(p.config).map(([k, v]) => ({ key: k, value: v == null ? '' : String(v) }))
}

function toConfigValues(p: Processor): Record<string, string> {
  const out: Record<string, string> = {}
  if (!p.config) return out
  for (const [k, v] of Object.entries(p.config)) out[k] = v == null ? '' : String(v)
  return out
}

function toConnRows(p: Processor): ConnRow[] {
  if (!p.connections) return []
  const out: ConnRow[] = []
  for (const [rel, ts] of Object.entries(p.connections)) {
    for (const t of ts ?? []) out.push({ rel, target: t })
  }
  return out
}

const DRAWER_WIDTH_KEY = 'zinc:drawer-width'
const DRAWER_MIN = 320
const DRAWER_MAX = 960

function loadDrawerWidth(): number {
  if (typeof window === 'undefined') return 420
  const raw = window.localStorage.getItem(DRAWER_WIDTH_KEY)
  const n = raw ? parseInt(raw, 10) : NaN
  if (!Number.isFinite(n)) return 420
  return Math.max(DRAWER_MIN, Math.min(DRAWER_MAX, n))
}

export function ProcessorDrawer({ processor, allProcessorNames, isSource = false, onClose }: Props) {
  const [tab, setTab] = useState<Tab>('config')

  // Resizable drawer: drag the left edge to widen. Width persists in
  // localStorage so the operator's preference carries between sessions.
  // draggingRef avoids re-renders on every mousemove; we only setState
  // when a new width snaps in.
  const [drawerWidth, setDrawerWidth] = useState<number>(loadDrawerWidth)
  const draggingRef = useRef(false)
  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      if (!draggingRef.current) return
      const w = Math.max(DRAWER_MIN, Math.min(DRAWER_MAX, window.innerWidth - e.clientX))
      setDrawerWidth(w)
    }
    const onUp = () => {
      if (draggingRef.current) {
        draggingRef.current = false
        document.body.style.cursor = ''
        document.body.style.userSelect = ''
      }
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
  }, [])
  useEffect(() => {
    window.localStorage.setItem(DRAWER_WIDTH_KEY, String(drawerWidth))
  }, [drawerWidth])
  const startDrag = (e: React.MouseEvent) => {
    e.preventDefault()
    draggingRef.current = true
    // Cursor + selection lock on body so the whole window reflects the
    // drag state, not just the handle. Restored in onUp.
    document.body.style.cursor = 'ew-resize'
    document.body.style.userSelect = 'none'
  }

  // View-model snapshot. Hydrated from the processor prop on mount +
  // when the drawer is pointed at a different processor. While
  // _dirty, upstream refreshes are queued and shown as a banner
  // instead of clobbering in-flight edits.
  const [configRows, setConfigRows] = useState<ConfigRow[]>(() => toConfigRows(processor))
  const [configValues, setConfigValues] = useState<Record<string, string>>(() => toConfigValues(processor))
  const [connRows, setConnRows] = useState<ConnRow[]>(() => toConnRows(processor))
  const [dirty, setDirty] = useState(false)
  const [upstreamQueued, setUpstreamQueued] = useState(false)
  const [status, setStatus] = useState<string | null>(null)

  // Registry drives the schema-aware config form (builder buttons on
  // Expression inputs, enum dropdowns, boolean checkboxes). Falls back
  // to the legacy row editor when the registry entry for this type
  // has no parameter descriptors (unknown type, older worker).
  const registry = useQuery<RegistryEntry[]>({
    queryKey: ['registry'],
    queryFn: api.registry,
    staleTime: 5 * 60_000,
  })
  const registryEntry = useMemo(() => {
    return (registry.data ?? []).find((e) => e.name === processor.type)
  }, [registry.data, processor.type])
  const schema = registryEntry?.parameters ?? []
  const wizardComponent = registryEntry?.wizardComponent ?? null
  const useSchemaForm = schema.length > 0

  const lastName = useRef(processor.name)

  useEffect(() => {
    const sameNode = lastName.current === processor.name
    if (dirty && sameNode) {
      setUpstreamQueued(true)
      return
    }
    setConfigRows(toConfigRows(processor))
    setConfigValues(toConfigValues(processor))
    setConnRows(toConnRows(processor))
    setDirty(false)
    setUpstreamQueued(false)
    setStatus(null)
    lastName.current = processor.name
  }, [processor, dirty])

  const updateConfig = useUpdateProcessorConfig()
  const updateSourceCfg = useUpdateSourceConfig()
  const setConns = useSetConnections()
  const toggleProc = useToggleProcessor()
  const removeProc = useRemoveProcessor()
  const removeSrc = useRemoveSource()

  const busy =
    updateConfig.isPending ||
    updateSourceCfg.isPending ||
    setConns.isPending ||
    toggleProc.isPending ||
    removeProc.isPending ||
    removeSrc.isPending

  const discardAndReload = () => {
    setConfigRows(toConfigRows(processor))
    setConfigValues(toConfigValues(processor))
    setConnRows(toConnRows(processor))
    setDirty(false)
    setUpstreamQueued(false)
    setStatus(null)
  }

  const saveConfig = async () => {
    setStatus(null)
    const config: Record<string, unknown> = {}
    if (useSchemaForm) {
      for (const [k, v] of Object.entries(configValues)) config[k] = v
    } else {
      for (const r of configRows) {
        if (!r.key.trim()) continue
        config[r.key] = r.value
      }
    }
    try {
      if (isSource) {
        await updateSourceCfg.mutateAsync({ name: processor.name, config })
      } else {
        await updateConfig.mutateAsync({ name: processor.name, type: processor.type, config })
      }
      setDirty(false)
      setStatus('applied')
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
    }
  }

  const saveConnections = async () => {
    setStatus(null)
    const rels: Record<string, string[]> = {}
    for (const r of connRows) {
      if (!r.rel.trim() || !r.target.trim()) continue
      ;(rels[r.rel] ??= []).push(r.target)
    }
    try {
      await setConns.mutateAsync({ from: processor.name, relationships: rels })
      setDirty(false)
      setStatus('applied')
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
    }
  }

  const onToggle = async () => {
    setStatus(null)
    try {
      await toggleProc.mutateAsync({ name: processor.name, enabled: processor.state !== 'ENABLED' })
      setStatus('toggled')
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
    }
  }

  const [confirmOpen, setConfirmOpen] = useState(false)
  const onDelete = () => {
    setStatus(null)
    setConfirmOpen(true)
  }
  const runDelete = async () => {
    setConfirmOpen(false)
    try {
      if (isSource) await removeSrc.mutateAsync(processor.name)
      else await removeProc.mutateAsync(processor.name)
      onClose()
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
    }
  }

  const statePillClass = useMemo(() => {
    switch (processor.state) {
      case 'ENABLED':
        return 'bg-[#0f3460] text-[var(--success)]'
      case 'STOPPED':
        return 'bg-[#4a2f10] text-[var(--warning)]'
      default:
        return 'bg-[var(--surface-2)] text-[var(--text-muted)]'
    }
  }, [processor.state])

  return (
    <>
    <ConfirmDialog
      open={confirmOpen}
      title={`Remove ${processor.name}?`}
      message={`${isSource ? 'Source' : 'Processor'} '${processor.name}' will be removed from the runtime graph. Its outbound connections drop with it.`}
      confirmLabel="remove"
      destructive
      onConfirm={runDelete}
      onCancel={() => setConfirmOpen(false)}
    />
    <aside
      className="absolute right-0 top-0 z-20 flex h-full flex-col shadow-xl"
      style={{ width: drawerWidth, background: 'var(--surface)', borderLeft: '1px solid var(--border)' }}
    >
      <div
        onMouseDown={startDrag}
        onDoubleClick={() => setDrawerWidth(420)}
        className="absolute left-0 top-0 h-full cursor-ew-resize"
        style={{ width: 6, marginLeft: -3, zIndex: 30 }}
        title="drag to resize · double-click to reset"
        aria-label="resize drawer"
        role="separator"
      />
      <header
        className="flex items-center justify-between px-4 py-3"
        style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)' }}
      >
        <div className="min-w-0 flex-1">
          <div className="truncate font-semibold text-white">{processor.name}</div>
          <div className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {processor.type}
            <span className={`ml-2 rounded-full px-2 py-0.5 text-[10px] uppercase tracking-widest ${statePillClass}`}>
              {processor.state}
            </span>
          </div>
        </div>
        <button
          onClick={onClose}
          className="text-xl leading-none"
          style={{ color: 'var(--text-muted)' }}
          aria-label="close"
        >
          ×
        </button>
      </header>

      {upstreamQueued && (
        <div
          className="flex items-center gap-2 px-4 py-2 text-[11px]"
          style={{ background: '#2a2410', borderBottom: '1px solid var(--warning)', color: 'var(--warning)' }}
        >
          <span className="flex-1">upstream changed — refresh to see latest</span>
          <button
            onClick={discardAndReload}
            className="rounded border px-2 py-0.5 text-[10px]"
            style={{ background: 'transparent', borderColor: 'var(--warning)', color: 'var(--warning)' }}
          >
            discard edits
          </button>
        </div>
      )}

      <nav className="flex border-b" style={{ borderColor: 'var(--border)', background: '#10102a' }}>
        {(['config', 'connections', 'stats', 'sample'] as Tab[]).map((t) => {
          const active = tab === t
          return (
            <button
              key={t}
              onClick={() => setTab(t)}
              className="flex-1 px-2 py-2 text-[12px] capitalize transition-colors"
              style={{
                color: active ? 'var(--accent)' : 'var(--text-muted)',
                borderBottom: active ? '2px solid var(--accent)' : '2px solid transparent',
                background: active ? 'var(--surface)' : 'transparent',
              }}
            >
              {t}
            </button>
          )
        })}
      </nav>

      <div className="flex-1 overflow-y-auto p-4 text-[12px]">
        {tab === 'config' && (
          <div className="flex flex-col gap-2">
            <h4 className="mt-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              type
            </h4>
            <div className="rounded" style={{ background: '#0a0a14', padding: '0.35rem 0.5rem' }}>
              {processor.type}
            </div>

            <h4 className="mt-4 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              config
            </h4>
            {useSchemaForm ? (
              <ProcessorConfigForm
                parameters={schema}
                values={configValues}
                onChange={(next) => { setConfigValues(next); setDirty(true) }}
                processorName={processor.name}
                wizardComponent={wizardComponent}
              />
            ) : (
              <>
            {configRows.length === 0 && (
              <p style={{ color: 'var(--text-muted)' }}>no config keys</p>
            )}
            {configRows.map((row, i) => (
              <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_24px] items-center gap-2">
                <input
                  value={row.key}
                  placeholder="key"
                  className="rounded border px-2 py-1"
                  style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                  onChange={(e) => {
                    const next = [...configRows]
                    next[i] = { ...row, key: e.target.value }
                    setConfigRows(next)
                    setDirty(true)
                  }}
                />
                <input
                  value={row.value}
                  placeholder="value"
                  className="rounded border px-2 py-1"
                  style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                  onChange={(e) => {
                    const next = [...configRows]
                    next[i] = { ...row, value: e.target.value }
                    setConfigRows(next)
                    setDirty(true)
                  }}
                />
                <button
                  onClick={() => {
                    setConfigRows(configRows.filter((_, j) => j !== i))
                    setDirty(true)
                  }}
                  className="text-lg leading-none"
                  style={{ color: 'var(--text-muted)' }}
                  aria-label="remove"
                >
                  ×
                </button>
              </div>
            ))}
            <button
              onClick={() => {
                setConfigRows([...configRows, { key: '', value: '' }])
                setDirty(true)
              }}
              className="self-start rounded border px-2 py-1"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
            >
              + add key
            </button>
              </>
            )}

            <div className="mt-4 flex gap-2">
              <button
                onClick={saveConfig}
                disabled={!dirty || busy}
                className="rounded px-3 py-1"
                style={{
                  background: dirty && !busy ? '#0f3460' : 'var(--surface-2)',
                  border: `1px solid ${dirty && !busy ? 'var(--accent)' : 'var(--border)'}`,
                  color: dirty && !busy ? '#fff' : 'var(--text-muted)',
                }}
              >
                {busy ? 'saving…' : 'apply to runtime'}
              </button>
              {dirty && (
                <button
                  onClick={discardAndReload}
                  className="rounded border px-3 py-1"
                  style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
                >
                  discard
                </button>
              )}
              {status && <span style={{ color: statusColor(status) }}>{statusLabel(status)}</span>}
            </div>
          </div>
        )}

        {tab === 'connections' && (
          <div className="flex flex-col gap-2">
            <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              outbound
            </h4>
            {connRows.length === 0 && (
              <p style={{ color: 'var(--text-muted)' }}>no outbound connections</p>
            )}
            {connRows.map((row, i) => (
              <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_24px] items-center gap-2">
                <input
                  value={row.rel}
                  placeholder="relationship"
                  className="rounded border px-2 py-1"
                  style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                  onChange={(e) => {
                    const next = [...connRows]
                    next[i] = { ...row, rel: e.target.value }
                    setConnRows(next)
                    setDirty(true)
                  }}
                />
                {(() => {
                  const stale = row.target.length > 0 && !allProcessorNames.includes(row.target)
                  return (
                    <select
                      value={row.target}
                      onChange={(e) => {
                        const next = [...connRows]
                        next[i] = { ...row, target: e.target.value }
                        setConnRows(next)
                        setDirty(true)
                      }}
                      className="rounded border px-2 py-1"
                      style={{
                        background: '#0a0a14',
                        borderColor: stale ? 'var(--error)' : 'var(--border)',
                        color: 'var(--text)',
                      }}
                      title={stale ? `target "${row.target}" no longer exists — pick another` : undefined}
                    >
                      <option value="">-- target --</option>
                      {stale && (
                        <option value={row.target}>{row.target} (removed)</option>
                      )}
                      {allProcessorNames
                        .filter((n) => n !== processor.name)
                        .map((n) => (
                          <option key={n} value={n}>
                            {n}
                          </option>
                        ))}
                    </select>
                  )
                })()}
                <button
                  onClick={() => {
                    setConnRows(connRows.filter((_, j) => j !== i))
                    setDirty(true)
                  }}
                  className="text-lg leading-none"
                  style={{ color: 'var(--text-muted)' }}
                  aria-label="remove"
                >
                  ×
                </button>
              </div>
            ))}
            <button
              onClick={() => {
                setConnRows([...connRows, { rel: '', target: '' }])
                setDirty(true)
              }}
              className="self-start rounded border px-2 py-1"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
            >
              + add connection
            </button>

            <div className="mt-4 flex gap-2">
              <button
                onClick={saveConnections}
                disabled={!dirty || busy}
                className="rounded px-3 py-1"
                style={{
                  background: dirty && !busy ? '#0f3460' : 'var(--surface-2)',
                  border: `1px solid ${dirty && !busy ? 'var(--accent)' : 'var(--border)'}`,
                  color: dirty && !busy ? '#fff' : 'var(--text-muted)',
                }}
              >
                {busy ? 'saving…' : 'apply to runtime'}
              </button>
              {dirty && (
                <button
                  onClick={discardAndReload}
                  className="rounded border px-3 py-1"
                  style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
                >
                  discard
                </button>
              )}
              {status && <span style={{ color: statusColor(status) }}>{statusLabel(status)}</span>}
            </div>
          </div>
        )}

        {tab === 'stats' && (
          <>
            <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
              <dt style={{ color: 'var(--text-muted)' }}>processed</dt>
              <dd>{processor.stats?.processed ?? 0}</dd>
              <dt style={{ color: 'var(--text-muted)' }}>errors</dt>
              <dd>{processor.stats?.errors ?? 0}</dd>
              <dt style={{ color: 'var(--text-muted)' }}>state</dt>
              <dd>{processor.state}</dd>
            </dl>
            <StatsResetButton name={processor.name} />
          </>
        )}

        {tab === 'sample' && <SamplePanel processorName={processor.name} />}
      </div>

      <footer
        className="flex items-center gap-2 px-4 py-2"
        style={{ background: '#10102a', borderTop: '1px solid var(--border)' }}
      >
        <button
          onClick={onDelete}
          disabled={busy}
          className="rounded border px-3 py-1"
          style={{ background: '#3a1a1a', borderColor: 'var(--error)', color: 'var(--error)' }}
        >
          delete
        </button>
        <button
          onClick={onToggle}
          disabled={busy}
          className="rounded border px-3 py-1"
          style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text)' }}
        >
          {processor.state === 'ENABLED' ? 'disable' : 'enable'}
        </button>
        <div className="flex-1" />
        {status && <span className="text-[11px]" style={{ color: statusColor(status) }}>{statusLabel(status)}</span>}
      </footer>
    </aside>
    </>
  )
}

function StatsResetButton({ name }: { name: string }) {
  const reset = useResetProcessorStats()
  const [msg, setMsg] = useState<string | null>(null)
  useEffect(() => {
    if (!msg) return
    const t = setTimeout(() => setMsg(null), 3000)
    return () => clearTimeout(t)
  }, [msg])
  const onReset = async () => {
    try {
      await reset.mutateAsync(name)
      setMsg('stats cleared')
    } catch (e) {
      setMsg(`error: ${(e as Error).message}`)
    }
  }
  return (
    <div className="mt-3 flex items-center gap-2">
      <button
        onClick={onReset}
        disabled={reset.isPending}
        className="rounded border px-2 py-1 text-[11px]"
        style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
      >
        {reset.isPending ? 'resetting…' : 'reset counters'}
      </button>
      {msg && (
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          {msg}
        </span>
      )}
    </div>
  )
}

// Peek: last 5 output samples from this processor. Each sample card
// shows timestamp, content-type header, a preview of the content, and
// a row of clickable field chips. Clicking a chip fires a window-level
// event the config tab listens for — its focused Expression /
// KeyValueList input appends the field path at the cursor. Keeping
// the handoff as an event avoids prop-drilling through every input.
function SamplePanel({ processorName }: { processorName: string }) {
  const query = useQuery({
    queryKey: ['processor-samples', processorName],
    queryFn: () => api.processorSamples(processorName),
    refetchInterval: 2_000,
    staleTime: 2_000,
  })

  if (query.isLoading)
    return <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading samples…</p>
  if (query.isError)
    return <p className="text-[12px]" style={{ color: 'var(--error)' }}>failed to load samples</p>

  const data = query.data
  if (!data || data.samples.length === 0) {
    return (
      <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
        {data?.sampling === false
          ? 'sampling disabled — set sampling.enabled on the worker'
          : 'no samples yet — send a flowfile through to peek at output'}
      </p>
    )
  }

  return (
    <div className="flex flex-col gap-3">
      {data.samples.map((s, idx) => {
        const fields = extractFields(s.contentType, s.preview, s.attributes)
        const ageMs = Date.now() - Math.floor((Date.now() - s.timestamp) / 1) // just display raw time diff
        return (
          <div
            key={`${s.timestamp}-${idx}`}
            className="rounded border p-2"
            style={{ background: '#0a0a14', borderColor: 'var(--border)' }}
          >
            <div className="mb-1 flex items-center justify-between text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              <span>{s.flowfile} · {s.contentType}</span>
              <span>{fmtAge(ageMs)}</span>
            </div>
            {fields.length > 0 && (
              <div className="mb-2 flex flex-wrap gap-1">
                {fields.map((f) => (
                  <button
                    key={f}
                    onClick={() => fireFieldPick(f)}
                    className="rounded px-1.5 py-0.5 text-[11px]"
                    style={{
                      background: 'var(--surface-2)',
                      border: '1px solid var(--border)',
                      color: 'var(--accent)',
                    }}
                    title={`insert ${f} into focused expression input`}
                  >
                    {f}
                  </button>
                ))}
              </div>
            )}
            <pre
              className="max-h-[200px] overflow-auto whitespace-pre-wrap text-[11px]"
              style={{ color: 'var(--text)' }}
            >
              {s.preview ?? '(binary — ' + (s.previewBase64?.length ?? 0) + ' base64 chars)'}
            </pre>
          </div>
        )
      })}
    </div>
  )
}

function statusColor(s: string): string {
  if (s.startsWith('error')) return 'var(--error)'
  if (s === 'applied' || s === 'toggled') return 'var(--success)'
  return 'var(--text-muted)'
}

function statusLabel(s: string): string {
  if (s === 'applied') return '✓ applied'
  if (s === 'toggled') return '✓ toggled'
  return s
}

function fmtAge(ms: number): string {
  if (ms < 1000) return 'just now'
  if (ms < 60_000) return `${Math.round(ms / 1000)}s ago`
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)}m ago`
  return `${Math.round(ms / 3_600_000)}h ago`
}

function extractFields(
  contentType: string,
  preview: string | null,
  attributes: Record<string, string>,
): string[] {
  const fields = new Set<string>()
  // Attributes are always field candidates — EL reads them with bare names.
  for (const k of Object.keys(attributes)) fields.add(k)
  // For record/json previews, pull the top-level keys out so users can
  // drop `amount`, `tier`, etc. directly into an expression.
  if (contentType === 'records' && preview) {
    try {
      const parsed = JSON.parse(preview)
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed))
        for (const k of Object.keys(parsed)) fields.add(k)
    } catch { /* preview may be truncated; field chips degrade gracefully */ }
  }
  return Array.from(fields).sort()
}

// Single window event the config tab listens for. Passing via callbacks
// would need to cross three layers of components (drawer → config tab →
// every FieldInput); an event is both simpler and matches DOM focus
// semantics — the currently-focused input picks up the insert.
function fireFieldPick(path: string) {
  window.dispatchEvent(new CustomEvent('zinc:field-pick', { detail: { path } }))
}
