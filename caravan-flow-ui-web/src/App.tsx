import { useState } from 'react'
import { AppShell, type View } from './layout/AppShell'
import { GraphPage } from './pages/GraphPage'
import { ErrorsPage } from './pages/ErrorsPage'
import { ProvenancePage } from './pages/ProvenancePage'
import { LineagePage } from './pages/LineagePage'
import { MetricsPage } from './pages/MetricsPage'
import { SettingsPage } from './pages/SettingsPage'

export function App() {
  const [view, setView] = useState<View>('graph')
  // Cross-view navigation: Provenance rows open Lineage focused on a
  // specific FlowFile. Kept as top-level state so the Lineage page can
  // hydrate its input + auto-load the ID on first render.
  const [lineageFocus, setLineageFocus] = useState<string | null>(null)

  const openLineage = (flowfile: string) => {
    setLineageFocus(flowfile)
    setView('lineage')
  }

  return (
    <AppShell current={view} onNavigate={setView}>
      {view === 'graph' && <GraphPage />}
      {view === 'lineage' && (
        <LineagePage
          initialId={lineageFocus}
          onClearInitial={() => setLineageFocus(null)}
        />
      )}
      {view === 'provenance' && <ProvenancePage onOpenLineage={openLineage} />}
      {view === 'errors' && <ErrorsPage onOpenLineage={openLineage} />}
      {view === 'settings' && <SettingsPage />}
      {view === 'metrics' && <MetricsPage />}
    </AppShell>
  )
}
