// Two-layer layout persistence:
//   - localStorage — per-user preference (user's own arrangement wins)
//   - server layout.yaml — team-shared fallback loaded at startup
//
// Read order on layoutFlow: localStorage → server → dagre auto-layout.
// Saving a drag updates localStorage immediately + debounced POST to the
// server so teammates opening the flow see a coherent default view.

const STORAGE_KEY = 'caravan.graph.positions.v1'

export interface Pos { x: number; y: number }

type PositionMap = Record<string, Pos>

// Server-sourced positions cached in-memory for the lifetime of the tab.
// GraphPage seeds this on mount via layoutStore.setServerPositions().
let serverPositions: PositionMap = {}

function read(): PositionMap {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw)
    return typeof parsed === 'object' && parsed ? parsed as PositionMap : {}
  } catch {
    return {}
  }
}

function write(next: PositionMap) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)) } catch { /* quota / SSR */ }
}

export const layoutStore = {
  get(name: string): Pos | undefined {
    return read()[name] ?? serverPositions[name]
  },
  getAll(): PositionMap {
    // Merge: server is the baseline, localStorage overrides per-user.
    return { ...serverPositions, ...read() }
  },
  set(name: string, pos: Pos) {
    const all = read()
    all[name] = pos
    write(all)
  },
  delete(name: string) {
    const all = read()
    delete all[name]
    write(all)
  },
  // Drop positions for processors that no longer exist — keeps storage
  // bounded as flows evolve.
  prune(existingNames: Set<string>) {
    const all = read()
    let changed = false
    for (const k of Object.keys(all)) {
      if (!existingNames.has(k)) { delete all[k]; changed = true }
    }
    if (changed) write(all)
  },
  // Merged snapshot to push to the server. Server writes what the user
  // sees, so teammates get the same view on first load.
  setServerPositions(positions: PositionMap) {
    serverPositions = positions ?? {}
  },
  getServerPositions(): PositionMap {
    return serverPositions
  },
}
