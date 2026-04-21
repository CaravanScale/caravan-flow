import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'

const POLL_MS = 5_000
const LIMIT = 100

function fmtTs(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return '—'
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
}

interface Props {
  onOpenLineage: (flowfile: string) => void
}

export function ProvenancePage({ onOpenLineage }: Props) {
  const q = useQuery({
    queryKey: ['provenance-recent', LIMIT],
    queryFn: () => api.provenanceRecent(LIMIT),
    refetchInterval: POLL_MS,
    staleTime: POLL_MS,
  })

  return (
    <div className="flex h-full flex-col p-6">
      <header className="mb-4">
        <h1 className="text-base font-semibold text-white">Provenance</h1>
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          last {LIMIT} events · polls every {POLL_MS / 1000}s
        </p>
      </header>

      {q.isError && (
        <div className="flex items-center gap-2 text-[12px]" style={{ color: 'var(--error)' }}>
          <span>failed to load /api/provenance: {(q.error as Error).message}</span>
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
      {q.isSuccess && q.data.length === 0 && (
        <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>no events recorded</p>
      )}
      {q.isSuccess && q.data.length > 0 && (
        <div className="flex-1 overflow-auto rounded border" style={{ borderColor: 'var(--border)' }}>
          <table className="w-full border-collapse text-left text-[12px]">
            <thead
              className="sticky top-0"
              style={{ background: 'var(--surface-2)', color: 'var(--text-muted)' }}
            >
              <tr>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">timestamp</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">type</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">flowfile</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">component</th>
                <th className="px-3 py-2 font-normal uppercase tracking-widest text-[10px]">details</th>
              </tr>
            </thead>
            <tbody>
              {q.data.map((e, i) => (
                <tr
                  key={`${e.timestamp}-${e.flowfile}-${i}`}
                  className="border-t"
                  style={{ borderColor: 'var(--border)' }}
                >
                  <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap">
                    {fmtTs(e.timestamp)}
                  </td>
                  <td className="px-3 py-1.5 font-mono text-[11px]">{e.type}</td>
                  <td className="px-3 py-1.5">
                    <button
                      onClick={() => onOpenLineage(e.flowfile)}
                      className="font-mono text-[11px] underline"
                      style={{ color: 'var(--accent)', background: 'transparent' }}
                      title="view lineage"
                    >
                      {e.flowfile}
                    </button>
                  </td>
                  <td className="px-3 py-1.5 font-mono text-[11px] whitespace-nowrap">{e.component}</td>
                  <td className="max-w-md truncate px-3 py-1.5" title={e.details}>{e.details}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
