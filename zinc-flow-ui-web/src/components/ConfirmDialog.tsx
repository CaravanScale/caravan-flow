interface Props {
  open: boolean
  title: string
  message: string
  confirmLabel?: string
  cancelLabel?: string
  destructive?: boolean
  onConfirm: () => void
  onCancel: () => void
}

export function ConfirmDialog({
  open,
  title,
  message,
  confirmLabel = 'confirm',
  cancelLabel = 'cancel',
  destructive = false,
  onConfirm,
  onCancel,
}: Props) {
  if (!open) return null
  return (
    <>
      <div className="fixed inset-0 z-40" style={{ background: 'rgba(0,0,0,0.5)' }} onClick={onCancel} />
      <div
        role="dialog"
        aria-modal="true"
        className="fixed left-1/2 top-1/2 z-50 flex w-[min(420px,92vw)] -translate-x-1/2 -translate-y-1/2 flex-col rounded-md shadow-2xl"
        style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
      >
        <header
          className="px-4 py-3"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)', borderRadius: '6px 6px 0 0' }}
        >
          <strong>{title}</strong>
        </header>
        <div className="p-4 text-[13px]" style={{ color: 'var(--text)' }}>{message}</div>
        <footer
          className="flex justify-end gap-2 px-4 py-3"
          style={{ background: 'var(--surface-2)', borderTop: '1px solid var(--border)', borderRadius: '0 0 6px 6px' }}
        >
          <button
            onClick={onCancel}
            className="rounded border px-3 py-1 text-[12px]"
            style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
          >
            {cancelLabel}
          </button>
          <button
            onClick={onConfirm}
            className="rounded px-3 py-1 text-[12px] font-semibold"
            style={{
              background: destructive ? 'var(--error)' : 'var(--accent)',
              color: '#fff',
              border: '1px solid transparent',
            }}
          >
            {confirmLabel}
          </button>
        </footer>
      </div>
    </>
  )
}
