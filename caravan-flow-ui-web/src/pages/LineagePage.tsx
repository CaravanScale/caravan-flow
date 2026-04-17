import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'

function fmtTs(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return '—'
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
}

// Accepts "ff-123" or "123" — the worker expects the numeric suffix.
function normalizeId(s: string): string {
  const t = s.trim()
  if (!t) return ''
  return t.startsWith('ff-') ? t.slice(3) : t
}

interface Props {
  initialId?: string | null
  onClearInitial?: () => void
}

export function LineagePage({ initialId, onClearInitial }: Props) {
  const [idText, setIdText] = useState<string>(initialId ?? '')
  const [loadedId, setLoadedId] = useState<string | null>(
    initialId ? normalizeId(initialId) : null,
  )

  useEffect(() => {
    if (initialId) {
      setIdText(initialId)
      setLoadedId(normalizeId(initialId))
      onClearInitial?.()
    }
  }, [initialId, onClearInitial])

  const q = useQuery({
    enabled: !!loadedId,
    queryKey: ['provenance-by-id', loadedId],
    queryFn: () => api.provenanceById(loadedId!),
  })

  const load = () => {
    const n = normalizeId(idText)
    if (!n) return
    setLoadedId(n)
  }

  return (
    <div className="flex h-full flex-col p-6">
      <header className="mb-4">
        <h1 className="text-base font-semibold text-white">Lineage</h1>
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          replay every provenance event for a single FlowFile
        </p>
      </header>

      <div className="mb-4 flex items-center gap-2">
        <label className="flex items-center gap-2 text-[12px]" style={{ color: 'var(--text-muted)' }}>
          FlowFile ID
          <input
            value={idText}
            onChange={(e) => setIdText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') load()
            }}
            placeholder="ff-123 or 123"
            className="rounded border px-2 py-1 font-mono text-[12px]"
            style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
          />
        </label>
        <button
          onClick={load}
          className="rounded px-3 py-1 text-[12px]"
          style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
        >
          load
        </button>
      </div>

      {!loadedId && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
          enter a FlowFile ID to replay its path.
        </p>
      )}
      {loadedId && q.isError && (
        <p className="text-[12px]" style={{ color: 'var(--error)' }}>
          failed to load: {(q.error as Error).message}
        </p>
      )}
      {loadedId && q.isLoading && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
      )}
      {loadedId && q.isSuccess && q.data.length === 0 && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>no events for ff-{loadedId}</p>
      )}
      {loadedId && q.isSuccess && q.data.length > 0 && (
        <div className="flex-1 overflow-auto rounded border" style={{ borderColor: 'var(--border)' }}>
          <table className="w-full border-collapse text-left text-[12px]">
            <thead
              className="sticky top-0"
              style={{ background: 'var(--surface-2)', color: 'var(--text-muted)' }}
            >
              <tr>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">timestamp</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">type</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">component</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">details</th>
              </tr>
            </thead>
            <tbody>
              {q.data.map((e, i) => (
                <tr
                  key={`${e.timestamp}-${e.type}-${i}`}
                  className="border-t"
                  style={{ borderColor: 'var(--border)' }}
                >
                  <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
                    {fmtTs(e.timestamp)}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-[11px]">{e.type}</td>
                  <td className="px-3 py-1.5 font-mono text-[11px]">{e.component}</td>
                  <td className="px-3 py-1.5">{e.details}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
