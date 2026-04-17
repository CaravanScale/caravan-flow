import type { ReactNode } from 'react'

type View = 'graph' | 'lineage' | 'provenance' | 'errors' | 'settings' | 'metrics'

const NAV: { id: View; label: string }[] = [
  { id: 'graph', label: 'Graph' },
  { id: 'lineage', label: 'Lineage' },
  { id: 'provenance', label: 'Provenance' },
  { id: 'errors', label: 'Errors' },
  { id: 'settings', label: 'Settings' },
  { id: 'metrics', label: 'Metrics' },
]

interface Props {
  current: View
  onNavigate: (v: View) => void
  children: ReactNode
}

export function AppShell({ current, onNavigate, children }: Props) {
  return (
    <div className="grid h-full" style={{ gridTemplateColumns: '200px 1fr' }}>
      <aside style={{ background: 'var(--surface)', borderRight: '1px solid var(--border)' }}>
        <div
          className="px-4 py-3 text-[14px] font-semibold"
          style={{ color: 'var(--accent)', borderBottom: '1px solid var(--border)' }}
        >
          caravan-flow
        </div>
        <nav className="flex flex-col">
          {NAV.map((item) => {
            const active = item.id === current
            return (
              <button
                key={item.id}
                onClick={() => onNavigate(item.id)}
                className="px-4 py-2 text-left transition-colors"
                style={{
                  color: active ? '#fff' : '#b0b0c0',
                  background: active ? 'var(--surface-2)' : 'transparent',
                  borderLeft: active ? '3px solid var(--accent)' : '3px solid transparent',
                }}
              >
                {item.label}
              </button>
            )
          })}
        </nav>
      </aside>
      <main className="relative">{children}</main>
    </div>
  )
}

export type { View }
