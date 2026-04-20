// Per-user processor layout persistence. NiFi-style UX: domain experts
// arrange boxes themselves, and that arrangement is a UI preference, not
// a flow property (see memory/project_ui_layout_is_ephemeral.md). We keep
// it in localStorage keyed by processor name; unknown processors fall
// back to dagre auto-layout on load.

const STORAGE_KEY = 'caravan.graph.positions.v1'

export interface Pos { x: number; y: number }

type PositionMap = Record<string, Pos>

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
    return read()[name]
  },
  getAll(): PositionMap {
    return read()
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
}
