import { useState } from 'react'
import { useRemoveConnection } from '../lib/mutations'
import { ConfirmDialog } from './ConfirmDialog'

export interface EdgeSelection {
  from: string
  relationship: string
  to: string
}

interface Props {
  edge: EdgeSelection
  onClose: () => void
}

export function EdgeDrawer({ edge, onClose }: Props) {
  const remove = useRemoveConnection()
  const [status, setStatus] = useState<string | null>(null)

  const [confirmOpen, setConfirmOpen] = useState(false)
  const onRemove = () => {
    setStatus(null)
    setConfirmOpen(true)
  }
  const runRemove = async () => {
    setConfirmOpen(false)
    try {
      await remove.mutateAsync(edge)
      onClose()
    } catch (e) {
      setStatus(`error: ${(e as Error).message}`)
    }
  }

  const relColor =
    edge.relationship === 'success'
      ? 'var(--success)'
      : edge.relationship === 'failure'
        ? 'var(--error)'
        : 'var(--warning)'

  return (
    <>
    <ConfirmDialog
      open={confirmOpen}
      title="Remove connection?"
      message={`${edge.from} --[${edge.relationship}]→ ${edge.to} will be dropped from the runtime graph.`}
      confirmLabel="remove"
      destructive
      onConfirm={runRemove}
      onCancel={() => setConfirmOpen(false)}
    />
    <aside
      className="absolute right-0 top-0 z-20 flex h-full w-[360px] flex-col shadow-xl"
      style={{ background: 'var(--surface)', borderLeft: '1px solid var(--border)' }}
    >
      <header
        className="flex items-center justify-between px-4 py-3"
        style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)' }}
      >
        <div className="min-w-0 flex-1 truncate text-[13px] text-white">
          <span className="font-semibold">{edge.from}</span>
          <span className="mx-2" style={{ color: 'var(--text-muted)' }}>→</span>
          <span className="font-semibold">{edge.to}</span>
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

      <div className="flex-1 overflow-y-auto p-4 text-[12px]">
        <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
          <dt style={{ color: 'var(--text-muted)' }}>relationship</dt>
          <dd>
            <span
              className="rounded px-2 py-0.5 text-[11px]"
              style={{ background: 'var(--surface-2)', color: relColor, border: `1px solid ${relColor}` }}
            >
              {edge.relationship}
            </span>
          </dd>
          <dt style={{ color: 'var(--text-muted)' }}>from</dt>
          <dd className="truncate">{edge.from}</dd>
          <dt style={{ color: 'var(--text-muted)' }}>to</dt>
          <dd className="truncate">{edge.to}</dd>
        </dl>
        <p className="mt-4" style={{ color: 'var(--text-muted)' }}>
          Removing drops this single edge from the runtime graph. For bulk rewiring use the
          source processor's Connections tab.
        </p>
      </div>

      <footer
        className="flex items-center gap-2 px-4 py-2"
        style={{ background: '#10102a', borderTop: '1px solid var(--border)' }}
      >
        <button
          onClick={onRemove}
          disabled={remove.isPending}
          className="rounded border px-3 py-1"
          style={{ background: '#3a1a1a', borderColor: 'var(--error)', color: 'var(--error)' }}
        >
          {remove.isPending ? 'removing…' : 'remove edge'}
        </button>
        <div className="flex-1" />
        {status && <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>{status}</span>}
      </footer>
    </aside>
    </>
  )
}
