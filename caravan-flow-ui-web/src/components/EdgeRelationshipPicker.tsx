import { useEffect, useState } from 'react'

// Popover shown when the operator drops a new edge onto a target node.
// Collects the relationship name — defaults to "success" which covers
// the majority of NiFi-style flows, but a dropdown surfaces the source
// processor's declared relationships (failure, unmatched, named routes)
// and a free-text field for custom ones (RouteOnAttribute-style).

interface Props {
  anchor: { x: number; y: number } | null
  suggestions: string[]
  onPick: (relationship: string) => void
  onCancel: () => void
}

export function EdgeRelationshipPicker({ anchor, suggestions, onPick, onCancel }: Props) {
  const [value, setValue] = useState('success')
  useEffect(() => {
    if (anchor) setValue(suggestions.includes('success') ? 'success' : (suggestions[0] ?? 'success'))
  }, [anchor, suggestions])

  useEffect(() => {
    if (!anchor) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onCancel() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [anchor, onCancel])

  if (!anchor) return null

  const uniqueSuggestions = Array.from(new Set(['success', 'failure', ...suggestions]))

  return (
    <>
      <div
        className="fixed inset-0 z-40"
        style={{ background: 'transparent' }}
        onClick={onCancel}
      />
      <div
        role="dialog"
        aria-modal="true"
        className="fixed z-50 rounded-md shadow-2xl"
        style={{
          left: anchor.x,
          top: anchor.y,
          background: 'var(--surface)',
          border: '1px solid var(--border)',
          width: 260,
        }}
      >
        <div
          className="px-3 py-2 text-[10px] uppercase tracking-widest"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)', color: 'var(--text-muted)' }}
        >
          relationship
        </div>
        <div className="flex flex-col gap-1.5 p-2">
          {uniqueSuggestions.map((s) => (
            <button
              key={s}
              onClick={() => onPick(s)}
              className="rounded border px-2 py-1 text-left text-[12px]"
              style={{
                background: s === value ? '#0f3460' : 'transparent',
                borderColor: s === value ? 'var(--accent)' : 'var(--border)',
                color: 'var(--text)',
              }}
            >
              {s}
            </button>
          ))}
          <div className="mt-1 flex items-center gap-1.5">
            <input
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="custom…"
              className="flex-1 rounded border px-2 py-1 text-[12px]"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
              onKeyDown={(e) => { if (e.key === 'Enter' && value.trim()) onPick(value.trim()) }}
            />
            <button
              disabled={!value.trim()}
              onClick={() => onPick(value.trim())}
              className="rounded px-2 py-1 text-[12px]"
              style={{
                background: value.trim() ? '#0f3460' : 'var(--surface-2)',
                border: `1px solid ${value.trim() ? 'var(--accent)' : 'var(--border)'}`,
                color: value.trim() ? '#fff' : 'var(--text-muted)',
              }}
            >
              connect
            </button>
          </div>
        </div>
      </div>
    </>
  )
}
