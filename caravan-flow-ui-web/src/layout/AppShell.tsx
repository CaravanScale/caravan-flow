import type { ReactNode } from 'react'
import { SaveButton } from '../components/SaveButton'

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
    <div className="grid h-full" style={{ gridTemplateColumns: '200px 1fr', gridTemplateRows: '42px 1fr' }}>
      <aside
        className="row-span-2"
        style={{ background: 'var(--surface)', borderRight: '1px solid var(--border)' }}
      >
        <div
          className="flex items-center gap-2 px-3 py-2"
          style={{ borderBottom: '1px solid var(--border)' }}
        >
          <img src="/logo-mark.png" alt="Caravan Flow" width={44} height={28} style={{ objectFit: 'contain' }} />
          <span className="text-[14px] font-semibold" style={{ color: 'var(--accent)' }}>
            caravan-flow
          </span>
        </div>
        <nav className="flex flex-col">
          {NAV.map((item) => {
            const active = item.id === current
            return (
              <button
                key={item.id}
                onClick={() => onNavigate(item.id)}
                aria-current={active ? 'page' : undefined}
                className="nav-btn px-4 py-2 text-left transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:-outline-offset-2"
                style={{
                  color: active ? '#fff' : 'var(--text-muted)',
                  background: active ? 'var(--surface-2)' : 'transparent',
                  borderLeft: active ? '3px solid var(--accent)' : '3px solid transparent',
                  outlineColor: 'var(--accent)',
                }}
              >
                {item.label}
              </button>
            )
          })}
        </nav>
      </aside>
      <header
        className="flex items-center justify-end gap-3 px-4"
        style={{ background: 'var(--surface)', borderBottom: '1px solid var(--border)' }}
      >
        <SaveButton />
      </header>
      <main className="relative overflow-hidden">{children}</main>
    </div>
  )
}

export type { View }
