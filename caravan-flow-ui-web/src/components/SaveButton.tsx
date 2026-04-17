import { useQuery } from '@tanstack/react-query'
import { useState, useEffect } from 'react'
import { api } from '../api/client'
import { useSaveFlow } from '../lib/mutations'

// Top-bar Save-to-config button. One click → worker writes
// config.yaml and (if a VersionControlProvider is enabled) commits +
// pushes. Badge shows clean/dirty/ahead/behind or "disk-only" when
// VC is off.

export function SaveButton() {
  const vc = useQuery({
    queryKey: ['vc-status'],
    queryFn: api.vcStatus,
    refetchInterval: 30_000,
    staleTime: 30_000,
  })
  const save = useSaveFlow()
  const [status, setStatus] = useState<string | null>(null)
  const [err, setErr] = useState(false)

  useEffect(() => {
    if (!status) return
    const t = setTimeout(() => setStatus(null), 6000)
    return () => clearTimeout(t)
  }, [status])

  const enabled = vc.data?.enabled ?? false
  const clean = vc.data?.clean ?? false
  const ahead = vc.data?.ahead ?? 0
  const behind = vc.data?.behind ?? 0
  const branch = vc.data?.branch ?? ''

  const onSave = async () => {
    setStatus(null); setErr(false)
    try {
      const body = await save.mutateAsync({ message: 'flow: update via UI', push: true })
      const parts: string[] = []
      if (typeof body.bytes === 'number') parts.push(`saved ${body.bytes}B`)
      else parts.push('saved')
      if (body.committed) parts.push('committed')
      if (body.pushed) parts.push('pushed')
      setStatus(parts.join(' · '))
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
      setErr(true)
    }
  }

  const pillBase = 'rounded-full border px-2.5 py-0.5 text-[11px] inline-flex items-center gap-1'

  return (
    <div className="flex items-center gap-2">
      {vc.data === undefined ? (
        <span className={pillBase} style={{ color: 'var(--text-muted)', borderColor: 'var(--border)' }}>
          VC: …
        </span>
      ) : !enabled ? (
        <span
          className={pillBase}
          title="VersionControlProvider not enabled — saves hit disk only"
          style={{ color: 'var(--text-muted)', borderColor: 'var(--border)' }}
        >
          disk-only
        </span>
      ) : (
        <span
          className={pillBase}
          title={`branch: ${branch}`}
          style={{
            color: clean && ahead === 0 && behind === 0 ? 'var(--success)' : 'var(--warning)',
            borderColor: clean && ahead === 0 && behind === 0 ? '#0f3460' : '#3a2f10',
          }}
        >
          VC: {branch || 'n/a'}
          {ahead > 0 && ` ↑${ahead}`}
          {behind > 0 && ` ↓${behind}`}
          {clean && ahead === 0 && behind === 0 && ' ✓'}
        </span>
      )}

      <button
        onClick={onSave}
        disabled={save.isPending}
        className="rounded px-3 py-1 text-[12px]"
        style={{
          background: '#0f3460',
          border: '1px solid var(--accent)',
          color: '#fff',
          opacity: save.isPending ? 0.6 : 1,
        }}
        title="writes config.yaml; commits + pushes if VC enabled"
      >
        {save.isPending ? 'saving…' : 'Save to config'}
      </button>

      {status && (
        <span className="text-[11px]" style={{ color: err ? 'var(--error)' : 'var(--text-muted)' }}>
          {status}
        </span>
      )}
    </div>
  )
}
