import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { RegistryEntry } from '../api/types'

// NiFi-style processor palette. Lists every registered type grouped by
// category; each item is HTML5-draggable onto the canvas. The drop
// handler (GraphPage) reads the MIME payload and creates the processor
// at the cursor position. Two-level collapse: each category's list can
// fold, and the whole palette can collapse to a narrow rail so the
// canvas gets the full viewport.

export const PALETTE_MIME = 'application/x-zinc-processor-type'

interface Props {
  collapsed: boolean
  onToggle: () => void
}

export function ProcessorPalette({ collapsed: collapsedAll, onToggle }: Props) {
  const registry = useQuery<RegistryEntry[]>({
    queryKey: ['registry'],
    queryFn: api.registry,
    staleTime: 5 * 60_000,
  })
  const [query, setQuery] = useState('')
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})

  const grouped = useMemo(() => {
    const list = registry.data ?? []
    const q = query.trim().toLowerCase()
    const filtered = q
      ? list.filter((e) =>
          e.name.toLowerCase().includes(q) || (e.description ?? '').toLowerCase().includes(q),
        )
      : list
    const byCat = new Map<string, RegistryEntry[]>()
    for (const e of filtered) {
      const cat = e.category ?? 'Other'
      if (!byCat.has(cat)) byCat.set(cat, [])
      byCat.get(cat)!.push(e)
    }
    return Array.from(byCat.entries())
      .map(([cat, items]) => [cat, items.slice().sort((a, b) => a.name.localeCompare(b.name))] as const)
      .sort((a, b) => CATEGORY_ORDER.indexOf(a[0]) - CATEGORY_ORDER.indexOf(b[0]))
  }, [registry.data, query])

  const onDragStart = (e: React.DragEvent<HTMLDivElement>, name: string) => {
    e.dataTransfer.setData(PALETTE_MIME, name)
    e.dataTransfer.effectAllowed = 'copy'
  }

  if (collapsedAll) {
    return (
      <aside
        className="flex h-full w-[32px] flex-col items-center"
        style={{ background: 'var(--surface)', borderRight: '1px solid var(--border)' }}
      >
        <button
          onClick={onToggle}
          className="mt-2 flex h-7 w-7 items-center justify-center rounded"
          style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-muted)' }}
          title="expand palette"
          aria-label="expand palette"
        >
          ▸
        </button>
        <div
          className="mt-4 text-[10px] uppercase tracking-widest"
          style={{
            color: 'var(--text)',
            writingMode: 'vertical-rl',
            transform: 'rotate(180deg)',
            letterSpacing: '0.2em',
          }}
        >
          processors
        </div>
      </aside>
    )
  }

  return (
    <aside
      className="flex h-full w-[240px] flex-col"
      style={{ background: 'var(--surface)', borderRight: '1px solid var(--border)' }}
    >
      <header
        className="px-3 py-2"
        style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)' }}
      >
        <div className="flex items-center justify-between">
          <div className="text-[11px] uppercase tracking-widest" style={{ color: 'var(--text)' }}>
            processors
          </div>
          <button
            onClick={onToggle}
            className="flex h-5 w-5 items-center justify-center rounded text-[11px]"
            style={{ color: 'var(--text)' }}
            title="collapse palette"
            aria-label="collapse palette"
          >
            ◂
          </button>
        </div>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="filter…"
          className="mt-1 w-full rounded border px-2 py-1 text-[12px]"
          style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
        />
      </header>
      <div className="flex-1 overflow-y-auto">
        {registry.isLoading && (
          <p className="p-3 text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
        )}
        {registry.isError && (
          <p className="p-3 text-[12px]" style={{ color: 'var(--error)' }}>failed to load /api/registry</p>
        )}
        {grouped.map(([category, items]) => {
          const isCollapsed = collapsed[category]
          return (
            <section key={category}>
              <button
                onClick={() => setCollapsed((c) => ({ ...c, [category]: !isCollapsed }))}
                className="flex w-full items-center justify-between px-3 py-1.5 text-left text-[10px] uppercase tracking-widest"
                style={{
                  background: '#10102a',
                  color: 'var(--text-muted)',
                  borderBottom: '1px solid var(--border)',
                }}
              >
                <span>{category}</span>
                <span>
                  <span className="mr-2">{items.length}</span>
                  <span>{isCollapsed ? '▸' : '▾'}</span>
                </span>
              </button>
              {!isCollapsed && items.map((e) => (
                <div
                  key={e.name}
                  draggable
                  onDragStart={(ev) => onDragStart(ev, e.name)}
                  className="cursor-grab border-b px-3 py-1.5 text-[12px]"
                  style={{ borderColor: 'var(--border)', color: 'var(--text)' }}
                  title={e.description ?? ''}
                >
                  <div className="font-medium">{e.name}</div>
                  {e.description && (
                    <div className="truncate text-[10px]" style={{ color: 'var(--text-muted)' }}>
                      {e.description}
                    </div>
                  )}
                </div>
              ))}
            </section>
          )
        })}
      </div>
      <footer
        className="px-3 py-2 text-[10px]"
        style={{ background: 'var(--surface-2)', borderTop: '1px solid var(--border)', color: 'var(--text-muted)' }}
      >
        drag onto canvas to add
      </footer>
    </aside>
  )
}

// Visual ordering for the palette. Unknown categories land at the end.
const CATEGORY_ORDER = [
  'Source', 'Attribute', 'Routing', 'Transform', 'Record',
  'Text', 'Utility', 'Conversion', 'Sink', 'V3', 'Other',
]
