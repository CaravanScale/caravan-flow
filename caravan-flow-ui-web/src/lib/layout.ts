import dagre from '@dagrejs/dagre'
import type { Node, Edge } from '@xyflow/react'
import type { Flow } from '../api/types'

const NODE_W = 220
const NODE_H = 88

export interface ProcessorNodeData extends Record<string, unknown> {
  processor: Flow['processors'][number]
  isEntry: boolean
  isSink: boolean
}

export interface RelEdgeData extends Record<string, unknown> {
  relationship: string
}

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

  const entrySet = new Set(flow.entryPoints ?? [])

  for (const p of flow.processors) {
    g.setNode(p.name, { width: NODE_W, height: NODE_H })
  }

  const rawEdges: { from: string; to: string; rel: string }[] = []
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

  const nodes: Node<ProcessorNodeData>[] = flow.processors.map((p) => {
    const laid = g.node(p.name)
    const isSink = !p.connections
      ? true
      : Object.values(p.connections).every((t) => !t || t.length === 0)
    return {
      id: p.name,
      type: 'processor',
      position: {
        x: (laid?.x ?? 0) - NODE_W / 2,
        y: (laid?.y ?? 0) - NODE_H / 2,
      },
      data: {
        processor: p,
        isEntry: entrySet.has(p.name),
        isSink,
      },
      draggable: false,
      selectable: true,
      sourcePosition: 'bottom' as const as unknown as Node['sourcePosition'],
      targetPosition: 'top' as const as unknown as Node['targetPosition'],
    }
  })

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
