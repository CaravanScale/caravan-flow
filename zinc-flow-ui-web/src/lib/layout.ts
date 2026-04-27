import dagre from '@dagrejs/dagre'
import type { Node, Edge } from '@xyflow/react'
import type { Flow } from '../api/types'
import { layoutStore } from './layoutStore'

export const NODE_W = 220
export const NODE_H = 88

export interface ProcessorNodeData extends Record<string, unknown> {
  processor: Flow['processors'][number]
  // Presentation hints for the NiFi-style graph model:
  //   kind = 'source'  → only an outbound handle, no inbound.
  //   kind = 'sink'    → only an inbound handle, no outbound.
  //   kind = 'processor' → both handles (default transform).
  kind: 'source' | 'processor' | 'sink'
  isEntry: boolean
  isSink: boolean
}

export interface RelEdgeData extends Record<string, unknown> {
  relationship: string
  // Recent-delta throughput for motion rendering. 0 = idle, positive =
  // animate. Set by the edge-stats poll in GraphPage, patched into the
  // edge's data without re-running dagre.
  motionDelta?: number
}

// Processor types whose category is "Sink" in the registry. Populated on
// first layoutFlow call that sees registry data; used to decide whether
// a processor node should render only a target handle (no source).
// Hard-coded list here so we don't need the full registry fetched before
// the first layout — these are stable built-in types.
const SINK_TYPES = new Set(['PutFile', 'PutHTTP', 'PutStdout'])

// Flatten the worker's flow response into React Flow nodes + edges
// with positions computed by dagre. Run once per topology change,
// never on every stats tick.
export function layoutFlow(flow: Flow): {
  nodes: Node<ProcessorNodeData>[]
  edges: Edge<RelEdgeData>[]
} {
  const g = new dagre.graphlib.Graph()
  g.setGraph({ rankdir: 'TB', ranksep: 70, nodesep: 40, marginx: 20, marginy: 20 })
  g.setDefaultEdgeLabel(() => ({}))

  // Sources + processors share the same id namespace in the canvas —
  // sources are just nodes with outbound-only handles.
  for (const s of flow.sources ?? []) {
    g.setNode(s.name, { width: NODE_W, height: NODE_H })
  }
  for (const p of flow.processors) {
    g.setNode(p.name, { width: NODE_W, height: NODE_H })
  }

  const rawEdges: { from: string; to: string; rel: string }[] = []
  for (const s of flow.sources ?? []) {
    if (!s.connections) continue
    for (const [rel, targets] of Object.entries(s.connections)) {
      for (const t of targets ?? []) {
        g.setEdge(s.name, t)
        rawEdges.push({ from: s.name, to: t, rel })
      }
    }
  }
  for (const p of flow.processors) {
    if (!p.connections) continue
    for (const [rel, targets] of Object.entries(p.connections)) {
      for (const t of targets ?? []) {
        g.setEdge(p.name, t)
        rawEdges.push({ from: p.name, to: t, rel })
      }
    }
  }

  dagre.layout(g)

  const saved = layoutStore.getAll()

  const sourceNodes: Node<ProcessorNodeData>[] = (flow.sources ?? []).map((s) => {
    const laid = g.node(s.name)
    const savedPos = saved[s.name]
    const pseudoProcessor: Flow['processors'][number] = {
      name: s.name,
      type: s.type,
      state: s.running ? 'ENABLED' : 'DISABLED',
      connections: s.connections ?? undefined,
      config: s.config ?? undefined,
      stats: undefined,
    }
    return {
      id: s.name,
      type: 'processor',
      position: savedPos ?? {
        x: (laid?.x ?? 0) - NODE_W / 2,
        y: (laid?.y ?? 0) - NODE_H / 2,
      },
      data: {
        processor: pseudoProcessor,
        kind: 'source',
        // Sources are the origin of the data — they are the entry points
        // in the tight model. Keep the badge semantics for users who
        // still expect it visually.
        isEntry: true,
        isSink: false,
      },
      draggable: true,
      selectable: true,
      sourcePosition: 'bottom' as const as unknown as Node['sourcePosition'],
      targetPosition: 'top' as const as unknown as Node['targetPosition'],
    }
  })

  const processorNodes: Node<ProcessorNodeData>[] = flow.processors.map((p) => {
    const laid = g.node(p.name)
    const savedPos = saved[p.name]
    const kind: 'source' | 'processor' | 'sink' = SINK_TYPES.has(p.type) ? 'sink' : 'processor'
    return {
      id: p.name,
      type: 'processor',
      position: savedPos ?? {
        x: (laid?.x ?? 0) - NODE_W / 2,
        y: (laid?.y ?? 0) - NODE_H / 2,
      },
      data: {
        processor: p,
        kind,
        isEntry: false,
        isSink: kind === 'sink',
      },
      draggable: true,
      selectable: true,
      sourcePosition: 'bottom' as const as unknown as Node['sourcePosition'],
      targetPosition: 'top' as const as unknown as Node['targetPosition'],
    }
  })

  const nodes = [...sourceNodes, ...processorNodes]

  const edges: Edge<RelEdgeData>[] = rawEdges.map(({ from, to, rel }) => ({
    id: `${from}::${rel}::${to}`,
    source: from,
    target: to,
    type: 'relationship',
    data: { relationship: rel },
    label: rel === 'success' ? undefined : rel,
    animated: false,
  }))

  return { nodes, edges }
}
