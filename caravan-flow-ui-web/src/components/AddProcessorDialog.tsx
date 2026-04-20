import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import { useAddProcessor } from '../lib/mutations'
import type { RegistryEntry } from '../api/types'
import { ProcessorConfigForm } from './ProcessorConfigForm'

interface Props {
  open: boolean
  existingNames: string[]
  onClose: () => void
}

// Add-processor dialog. Registry (fetched once on open) drives the type
// dropdown and, when a type is picked, the config form. Processors that
// advertise typed parameters get a schema-driven form; legacy entries
// (no parameters array) fall through to the generic key/value grid.

interface LegacyRow { key: string; value: string }

export function AddProcessorDialog({ open, existingNames, onClose }: Props) {
  const registry = useQuery<RegistryEntry[]>({
    queryKey: ['registry'],
    queryFn: api.registry,
    enabled: open,
    staleTime: 5 * 60_000,
  })
  const addProc = useAddProcessor()

  const [name, setName] = useState('')
  const [type, setType] = useState('')
  const [values, setValues] = useState<Record<string, string>>({})
  const [legacyRows, setLegacyRows] = useState<LegacyRow[]>([])
  const [query, setQuery] = useState('')
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    if (!open) {
      setName(''); setType(''); setValues({}); setLegacyRows([]); setQuery(''); setErr(null)
    }
  }, [open])

  const filteredRegistry = useMemo(() => {
    const list = registry.data ?? []
    if (!query.trim()) return list
    const q = query.toLowerCase()
    return list.filter((e) => e.name.toLowerCase().includes(q) || (e.description ?? '').toLowerCase().includes(q))
  }, [registry.data, query])

  const selectedEntry = useMemo(
    () => (registry.data ?? []).find((e) => e.name === type),
    [registry.data, type]
  )
  const hasTypedParams = (selectedEntry?.parameters?.length ?? 0) > 0

  const onPickType = (t: string) => {
    setType(t)
    const entry = (registry.data ?? []).find((e) => e.name === t)
    if (entry?.parameters?.length) {
      const seed: Record<string, string> = {}
      for (const p of entry.parameters) seed[p.name] = p.default ?? ''
      setValues(seed)
      setLegacyRows([])
    } else {
      setValues({})
      setLegacyRows(entry ? entry.configKeys.map((k) => ({ key: k, value: '' })) : [])
    }
  }

  const requiredSatisfied = useMemo(() => {
    if (!selectedEntry?.parameters?.length) return true
    return selectedEntry.parameters.every((p) => !p.required || (values[p.name] ?? '').trim().length > 0)
  }, [selectedEntry, values])

  const canSubmit =
    name.trim().length > 0 &&
    !existingNames.includes(name.trim()) &&
    type.trim().length > 0 &&
    requiredSatisfied &&
    !addProc.isPending

  const submit = async () => {
    setErr(null)
    const cfg: Record<string, unknown> = {}
    if (hasTypedParams && selectedEntry?.parameters) {
      for (const p of selectedEntry.parameters) {
        const v = values[p.name]
        if (v === undefined) continue
        if (!p.required && v.trim().length === 0) continue
        cfg[p.name] = v
      }
    } else {
      for (const r of legacyRows) {
        if (!r.key.trim()) continue
        cfg[r.key] = r.value
      }
    }
    try {
      await addProc.mutateAsync({
        name: name.trim(),
        type,
        config: cfg,
        connections: {},
      })
      onClose()
    } catch (e) {
      setErr((e as Error).message)
    }
  }

  if (!open) return null

  const nameConflict = name.trim().length > 0 && existingNames.includes(name.trim())

  return (
    <>
      <div
        className="fixed inset-0 z-40"
        style={{ background: 'rgba(0,0,0,0.5)' }}
        onClick={onClose}
      />
      <div
        role="dialog"
        aria-modal="true"
        className="fixed left-1/2 top-1/2 z-50 flex max-h-[90vh] w-[min(640px,95vw)] -translate-x-1/2 -translate-y-1/2 flex-col rounded-md shadow-2xl"
        style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
      >
        <header
          className="flex items-center justify-between px-4 py-3"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)', borderRadius: '6px 6px 0 0' }}
        >
          <strong>Add processor</strong>
          <button
            onClick={onClose}
            className="text-xl leading-none"
            style={{ color: 'var(--text-muted)' }}
            aria-label="close"
          >
            ×
          </button>
        </header>

        <div className="flex-1 overflow-y-auto p-4">
          <div className="mb-4">
            <label className="mb-1 block text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              name
            </label>
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="unique name (e.g. route-env)"
              className="w-full rounded border px-2 py-1"
              style={{ background: '#0a0a14', borderColor: nameConflict ? 'var(--error)' : 'var(--border)', color: 'var(--text)' }}
            />
            {nameConflict && (
              <p className="mt-1 text-[11px]" style={{ color: 'var(--error)' }}>
                a processor named "{name}" already exists
              </p>
            )}
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              type
            </label>
            {registry.isLoading && <p style={{ color: 'var(--text-muted)' }}>loading registry…</p>}
            {registry.isError && <p style={{ color: 'var(--error)' }}>failed to load /api/registry</p>}
            {registry.data && (
              <>
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="search…"
                  className="mb-2 w-full rounded border px-2 py-1"
                  style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                />
                <div
                  className="max-h-[220px] overflow-y-auto rounded border"
                  style={{ borderColor: 'var(--border)' }}
                >
                  {filteredRegistry.length === 0 && (
                    <p className="p-2" style={{ color: 'var(--text-muted)' }}>
                      no matches
                    </p>
                  )}
                  {filteredRegistry.map((e) => {
                    const active = e.name === type
                    return (
                      <button
                        key={e.name}
                        onClick={() => onPickType(e.name)}
                        className="flex w-full flex-col items-start gap-0.5 border-b px-3 py-2 text-left"
                        style={{
                          background: active ? 'var(--surface-2)' : 'transparent',
                          borderColor: 'var(--border)',
                          borderLeft: active ? '3px solid var(--accent)' : '3px solid transparent',
                        }}
                      >
                        <span className="flex w-full items-baseline justify-between gap-2">
                          <span className="font-semibold text-white">{e.name}</span>
                          {e.category && (
                            <span
                              className="text-[10px] uppercase tracking-widest"
                              style={{ color: 'var(--text-muted)' }}
                            >
                              {e.category}
                            </span>
                          )}
                        </span>
                        {e.description && (
                          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
                            {e.description}
                          </span>
                        )}
                      </button>
                    )
                  })}
                </div>
              </>
            )}
          </div>

          {type && (
            <div className="mb-4">
              <label className="mb-1 block text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
                config
              </label>
              {hasTypedParams ? (
                <ProcessorConfigForm
                  parameters={selectedEntry!.parameters!}
                  values={values}
                  onChange={setValues}
                />
              ) : (
                <LegacyKeyValueGrid rows={legacyRows} setRows={setLegacyRows} typePicked={type.length > 0} />
              )}
            </div>
          )}

          {err && (
            <p className="text-[11px]" style={{ color: 'var(--error)' }}>
              {err}
            </p>
          )}
        </div>

        <footer
          className="flex items-center gap-2 px-4 py-3"
          style={{ background: '#10102a', borderTop: '1px solid var(--border)', borderRadius: '0 0 6px 6px' }}
        >
          <button
            onClick={onClose}
            className="rounded border px-3 py-1"
            style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
          >
            cancel
          </button>
          <div className="flex-1" />
          <button
            onClick={submit}
            disabled={!canSubmit}
            className="rounded px-4 py-1"
            style={{
              background: canSubmit ? '#0f3460' : 'var(--surface-2)',
              border: `1px solid ${canSubmit ? 'var(--accent)' : 'var(--border)'}`,
              color: canSubmit ? '#fff' : 'var(--text-muted)',
            }}
          >
            {addProc.isPending ? 'adding…' : 'add'}
          </button>
        </footer>
      </div>
    </>
  )
}

interface LegacyProps {
  rows: LegacyRow[]
  setRows: (rows: LegacyRow[]) => void
  typePicked: boolean
}

function LegacyKeyValueGrid({ rows, setRows, typePicked }: LegacyProps) {
  return (
    <>
      {rows.length === 0 && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
          {typePicked ? 'no known config keys — add any below' : 'pick a type to seed config keys'}
        </p>
      )}
      <div className="flex flex-col gap-1.5">
        {rows.map((row, i) => (
          <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_24px] items-center gap-2">
            <input
              value={row.key}
              placeholder="key"
              onChange={(e) => {
                const next = [...rows]
                next[i] = { ...row, key: e.target.value }
                setRows(next)
              }}
              className="rounded border px-2 py-1"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
            />
            <input
              value={row.value}
              placeholder="value"
              onChange={(e) => {
                const next = [...rows]
                next[i] = { ...row, value: e.target.value }
                setRows(next)
              }}
              className="rounded border px-2 py-1"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
            />
            <button
              onClick={() => setRows(rows.filter((_, j) => j !== i))}
              className="text-lg leading-none"
              style={{ color: 'var(--text-muted)' }}
              aria-label="remove"
              type="button"
            >
              ×
            </button>
          </div>
        ))}
      </div>
      <button
        onClick={() => setRows([...rows, { key: '', value: '' }])}
        className="mt-2 rounded border px-2 py-1 text-[11px]"
        style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
        type="button"
      >
        + add key
      </button>
    </>
  )
}
