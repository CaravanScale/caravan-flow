import { useQuery } from '@tanstack/react-query'
import { useState, useEffect } from 'react'
import { api } from '../api/client'
import { useStartSource, useStopSource } from '../lib/mutations'
import type { SourceInfo } from '../api/types'

// Source lifecycle panel. Connector sources (GetFile, ListenHTTP, TestPoller)
// live outside the processor graph — the fabric owns them. This panel shows
// each registered source's running state and lets operators start/stop them.
// Polls /api/sources every 4s so a stopped source reflects quickly.

interface Props {
  onClose: () => void
}

export function SourcesPanel({ onClose }: Props) {
  const sources = useQuery<SourceInfo[]>({
    queryKey: ['sources'],
    queryFn: api.sources,
    refetchInterval: 4_000,
    staleTime: 4_000,
  })
  const start = useStartSource()
  const stop = useStopSource()
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    if (!err) return
    const t = setTimeout(() => setErr(null), 4000)
    return () => clearTimeout(t)
  }, [err])

  const onStart = async (name: string) => {
    setErr(null)
    try { await start.mutateAsync(name) } catch (e) { setErr((e as Error).message) }
  }
  const onStop = async (name: string) => {
    setErr(null)
    try { await stop.mutateAsync(name) } catch (e) { setErr((e as Error).message) }
  }

  return (
    <>
      <div className="fixed inset-0 z-40" style={{ background: 'rgba(0,0,0,0.5)' }} onClick={onClose} />
      <aside
        role="dialog"
        aria-modal="true"
        className="fixed right-0 top-0 z-50 flex h-full w-[min(420px,95vw)] flex-col"
        style={{ background: 'var(--surface)', borderLeft: '1px solid var(--border)' }}
      >
        <header
          className="flex items-center justify-between px-4 py-3"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)' }}
        >
          <strong>Sources</strong>
          <button onClick={onClose} className="text-xl leading-none" style={{ color: 'var(--text-muted)' }} aria-label="close">×</button>
        </header>
        <div className="flex-1 overflow-y-auto p-4 text-[13px]">
          {sources.isLoading && <p style={{ color: 'var(--text-muted)' }}>loading…</p>}
          {sources.isError && <p style={{ color: 'var(--error)' }}>failed to load /api/sources</p>}
          {sources.data && sources.data.length === 0 && (
            <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
              no sources configured — add one to the flow config.yaml (GetFile, ListenHTTP, TestPoller).
            </p>
          )}
          <div className="flex flex-col gap-2">
            {(sources.data ?? []).map((s) => (
              <div
                key={s.name}
                className="flex items-center gap-3 rounded border p-3"
                style={{ background: '#0a0a14', borderColor: 'var(--border)' }}
              >
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ background: s.running ? 'var(--success)' : 'var(--text-muted)' }}
                  aria-label={s.running ? 'running' : 'stopped'}
                />
                <div className="flex flex-1 flex-col">
                  <span className="font-semibold text-white">{s.name}</span>
                  <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>{s.type}</span>
                </div>
                {s.running ? (
                  <button
                    onClick={() => onStop(s.name)}
                    disabled={stop.isPending}
                    className="rounded border px-2 py-1 text-[11px]"
                    style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text)' }}
                  >
                    stop
                  </button>
                ) : (
                  <button
                    onClick={() => onStart(s.name)}
                    disabled={start.isPending}
                    className="rounded px-2 py-1 text-[11px]"
                    style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
                  >
                    start
                  </button>
                )}
              </div>
            ))}
          </div>
          {err && <p className="mt-3 text-[11px]" style={{ color: 'var(--error)' }}>{err}</p>}
        </div>
      </aside>
    </>
  )
}
