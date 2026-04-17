import { useState } from 'react'
import { AppShell, type View } from './layout/AppShell'
import { GraphPage } from './pages/GraphPage'

// First-commit shape: only the Graph view is real. Other views are
// "coming soon" placeholders so the navigation renders and the app
// compiles end-to-end. Each placeholder turns into its own file
// (src/pages/*.tsx) as the slices land.

function Placeholder({ label }: { label: string }) {
  return (
    <div className="p-6">
      <h1 className="text-base font-semibold text-white">{label}</h1>
      <p className="mt-2 text-[12px]" style={{ color: 'var(--text-muted)' }}>
        landing in a follow-up slice — Graph view is the first commit of the React migration.
      </p>
    </div>
  )
}

export function App() {
  const [view, setView] = useState<View>('graph')

  return (
    <AppShell current={view} onNavigate={setView}>
      {view === 'graph' && <GraphPage />}
      {view === 'lineage' && <Placeholder label="Lineage" />}
      {view === 'provenance' && <Placeholder label="Provenance" />}
      {view === 'errors' && <Placeholder label="Errors" />}
      {view === 'settings' && <Placeholder label="Settings" />}
      {view === 'metrics' && <Placeholder label="Metrics" />}
    </AppShell>
  )
}
