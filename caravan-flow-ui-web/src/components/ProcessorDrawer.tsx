import { useEffect, useMemo, useRef, useState } from 'react'
import type { Processor } from '../api/types'
import {
  useRemoveProcessor,
  useSetConnections,
  useSetEntryPoints,
  useToggleProcessor,
  useUpdateProcessorConfig,
} from '../lib/mutations'

type Tab = 'config' | 'connections' | 'stats'

interface Props {
  processor: Processor
  allProcessorNames: string[]
  entryPoints: string[]
  onClose: () => void
}

interface ConfigRow { key: string; value: string }
interface ConnRow { rel: string; target: string }

function toConfigRows(p: Processor): ConfigRow[] {
  if (!p.config) return []
  return Object.entries(p.config).map(([k, v]) => ({ key: k, value: v == null ? '' : String(v) }))
}

function toConnRows(p: Processor): ConnRow[] {
  if (!p.connections) return []
  const out: ConnRow[] = []
  for (const [rel, ts] of Object.entries(p.connections)) {
    for (const t of ts ?? []) out.push({ rel, target: t })
  }
  return out
}

export function ProcessorDrawer({ processor, allProcessorNames, entryPoints, onClose }: Props) {
  const [tab, setTab] = useState<Tab>('config')

  // View-model snapshot. Hydrated from the processor prop on mount +
  // when the drawer is pointed at a different processor. While
  // _dirty, upstream refreshes are queued and shown as a banner
  // instead of clobbering in-flight edits.
  const [configRows, setConfigRows] = useState<ConfigRow[]>(() => toConfigRows(processor))
  const [connRows, setConnRows] = useState<ConnRow[]>(() => toConnRows(processor))
  const [isEntry, setIsEntry] = useState<boolean>(() => entryPoints.includes(processor.name))
  const [dirty, setDirty] = useState(false)
  const [upstreamQueued, setUpstreamQueued] = useState(false)
  const [status, setStatus] = useState<string | null>(null)

  const lastName = useRef(processor.name)

  useEffect(() => {
    const sameNode = lastName.current === processor.name
    if (dirty && sameNode) {
      setUpstreamQueued(true)
      return
    }
    setConfigRows(toConfigRows(processor))
    setConnRows(toConnRows(processor))
    setIsEntry(entryPoints.includes(processor.name))
    setDirty(false)
    setUpstreamQueued(false)
    setStatus(null)
    lastName.current = processor.name
  }, [processor, entryPoints, dirty])

  const updateConfig = useUpdateProcessorConfig()
  const setConns = useSetConnections()
  const setEntries = useSetEntryPoints()
  const toggleProc = useToggleProcessor()
  const removeProc = useRemoveProcessor()

  const busy =
    updateConfig.isPending ||
    setConns.isPending ||
    setEntries.isPending ||
    toggleProc.isPending ||
    removeProc.isPending

  const discardAndReload = () => {
    setConfigRows(toConfigRows(processor))
    setConnRows(toConnRows(processor))
    setIsEntry(entryPoints.includes(processor.name))
    setDirty(false)
    setUpstreamQueued(false)
    setStatus(null)
  }

  const saveConfig = async () => {
    setStatus(null)
    const config: Record<string, unknown> = {}
    for (const r of configRows) {
      if (!r.key.trim()) continue
      config[r.key] = r.value
    }
    try {
      await updateConfig.mutateAsync({ name: processor.name, type: processor.type, config })
      const currentIsEntry = entryPoints.includes(processor.name)
      if (currentIsEntry !== isEntry) {
        const next = isEntry
          ? Array.from(new Set([...entryPoints, processor.name]))
          : entryPoints.filter((n) => n !== processor.name)
        await setEntries.mutateAsync(next)
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

  const onDelete = async () => {
    setStatus(null)
    if (!confirm(`Remove ${processor.name} from the runtime graph?`)) return
    try {
      await removeProc.mutateAsync(processor.name)
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
        return 'bg-[#3a2f10] text-[var(--warning)]'
      default:
        return 'bg-[var(--surface-2)] text-[var(--text-muted)]'
    }
  }, [processor.state])

  return (
    <aside
      className="absolute right-0 top-0 z-20 flex h-full w-[420px] flex-col shadow-xl"
      style={{ background: 'var(--surface)', borderLeft: '1px solid var(--border)' }}
    >
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
          className="px-4 py-2 text-[11px]"
          style={{ background: '#2a2410', borderBottom: '1px solid var(--warning)', color: 'var(--warning)' }}
        >
          upstream changed —{' '}
          <button className="underline" onClick={discardAndReload}>
            discard edits
          </button>{' '}
          to see latest
        </div>
      )}

      <nav className="flex border-b" style={{ borderColor: 'var(--border)', background: '#10102a' }}>
        {(['config', 'connections', 'stats'] as Tab[]).map((t) => {
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
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={isEntry}
                onChange={(e) => {
                  setIsEntry(e.target.checked)
                  setDirty(true)
                }}
              />
              entry point
            </label>

            <h4 className="mt-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              type
            </h4>
            <div className="rounded" style={{ background: '#0a0a14', padding: '0.35rem 0.5rem' }}>
              {processor.type}
            </div>

            <h4 className="mt-4 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              config
            </h4>
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
              {status && <span style={{ color: 'var(--text-muted)' }}>{status}</span>}
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
                <select
                  value={row.target}
                  onChange={(e) => {
                    const next = [...connRows]
                    next[i] = { ...row, target: e.target.value }
                    setConnRows(next)
                    setDirty(true)
                  }}
                  className="rounded border px-2 py-1"
                  style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                >
                  <option value="">-- target --</option>
                  {allProcessorNames
                    .filter((n) => n !== processor.name)
                    .map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                </select>
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
              {status && <span style={{ color: 'var(--text-muted)' }}>{status}</span>}
            </div>
          </div>
        )}

        {tab === 'stats' && (
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
            <dt style={{ color: 'var(--text-muted)' }}>processed</dt>
            <dd>{processor.stats?.processed ?? 0}</dd>
            <dt style={{ color: 'var(--text-muted)' }}>errors</dt>
            <dd>{processor.stats?.errors ?? 0}</dd>
            <dt style={{ color: 'var(--text-muted)' }}>state</dt>
            <dd>{processor.state}</dd>
          </dl>
        )}
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
        {status && <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>{status}</span>}
      </footer>
    </aside>
  )
}
