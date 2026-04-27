import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'

const POLL_MS = 5_000

interface Sample { name: string; labels: string; value: string }

// Prometheus text-exposition parser. Skips HELP/TYPE comment lines,
// extracts `name{labels} value [timestamp]` or `name value` shapes.
function parse(text: string): Sample[] {
  const out: Sample[] = []
  for (const raw of text.split('\n')) {
    const l = raw.trim()
    if (!l || l[0] === '#') continue
    const braceOpen = l.indexOf('{')
    let name: string
    let labels: string
    let valueStart: number
    if (braceOpen >= 0) {
      const braceClose = l.indexOf('}', braceOpen)
      if (braceClose < 0) continue
      name = l.slice(0, braceOpen)
      labels = l.slice(braceOpen + 1, braceClose)
      valueStart = braceClose + 1
    } else {
      const sp = l.indexOf(' ')
      if (sp < 0) continue
      name = l.slice(0, sp)
      labels = ''
      valueStart = sp
    }
    const value = l.slice(valueStart).trim().split(' ')[0]
    out.push({ name, labels, value })
  }
  return out
}

export function MetricsPage() {
  const q = useQuery({
    queryKey: ['metrics'],
    queryFn: api.metrics,
    refetchInterval: POLL_MS,
    staleTime: POLL_MS,
  })

  const [filter, setFilter] = useState('')
  const samples = useMemo<Sample[]>(() => (q.data ? parse(q.data) : []), [q.data])
  const filtered = useMemo(() => {
    if (!filter.trim()) return samples
    const needle = filter.toLowerCase()
    return samples.filter(
      (s) => s.name.toLowerCase().includes(needle) || s.labels.toLowerCase().includes(needle),
    )
  }, [samples, filter])

  return (
    <div className="flex h-full flex-col p-6">
      <header className="mb-4">
        <h1 className="text-base font-semibold text-white">Metrics</h1>
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          Prometheus text exposition · polls every {POLL_MS / 1000}s · {samples.length} samples
        </p>
      </header>

      <div className="mb-3 flex items-center gap-2">
        <input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="filter by metric name or label…"
          className="w-full max-w-md rounded border px-2 py-1 font-mono text-[12px]"
          style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
        />
      </div>

      {q.isError && (
        <div className="flex items-center gap-2 text-[12px]" style={{ color: 'var(--error)' }}>
          <span>failed to load /metrics: {(q.error as Error).message}</span>
          <button
            onClick={() => q.refetch()}
            className="rounded border px-2 py-0.5 text-[11px]"
            style={{ background: 'transparent', borderColor: 'var(--error)', color: 'var(--error)' }}
          >
            retry
          </button>
        </div>
      )}
      {q.isLoading && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
      )}
      {q.isSuccess && filtered.length === 0 && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
          {samples.length === 0 ? 'no metrics exported' : 'no matches for current filter'}
        </p>
      )}
      {q.isSuccess && filtered.length > 0 && (
        <div className="flex-1 overflow-auto rounded border" style={{ borderColor: 'var(--border)' }}>
          <table className="w-full border-collapse text-left text-[12px]">
            <thead
              className="sticky top-0"
              style={{ background: 'var(--surface-2)', color: 'var(--text-muted)' }}
            >
              <tr>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">metric</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">labels</th>
                <th className="px-3 py-2 text-right font-normal uppercase tracking-widest text-[10px]">value</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((s, i) => (
                <tr key={i} className="border-t" style={{ borderColor: 'var(--border)' }}>
                  <td className="px-3 py-1.5 font-mono text-[11px]">{s.name}</td>
                  <td className="px-3 py-1.5 font-mono text-[11px]" style={{ color: 'var(--text-muted)' }}>
                    {s.labels}
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-[11px]">{s.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
