import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
  type NodeMouseHandler,
} from '@xyflow/react'
import { api } from '../api/client'
import { layoutFlow, type ProcessorNodeData, type RelEdgeData } from '../lib/layout'
import { ProcessorNode } from '../components/ProcessorNode'
import { RelationshipEdge } from '../components/RelationshipEdge'
import type { Processor } from '../api/types'

const nodeTypes = { processor: ProcessorNode }
const edgeTypes = { relationship: RelationshipEdge }

// Topology poll (slow): 15 s. Stats poll (fast): 2 s.
// Topology poll re-runs dagre only when the underlying shape changes
// (dep on the query data through React Flow's nodes array identity).
// Stats poll mutates node.data in place — React Flow re-renders the
// node but doesn't re-layout.
const TOPOLOGY_POLL_MS = 15_000
const STATS_POLL_MS = 2_000

export function GraphPage() {
  const topology = useQuery({
    queryKey: ['flow'],
    queryFn: api.flow,
    refetchInterval: TOPOLOGY_POLL_MS,
    staleTime: TOPOLOGY_POLL_MS,
  })
  const stats = useQuery({
    queryKey: ['processor-stats'],
    queryFn: api.processorStats,
    refetchInterval: STATS_POLL_MS,
    staleTime: STATS_POLL_MS,
  })

  const [selected, setSelected] = useState<string | null>(null)

  // Lay out from the topology, then fold in live stats so the node
  // cards show fresh numbers without forcing a new dagre pass.
  const { nodes, edges } = useMemo<{
    nodes: Node<ProcessorNodeData>[]
    edges: Edge<RelEdgeData>[]
  }>(() => {
    if (!topology.data) return { nodes: [], edges: [] }
    const laid = layoutFlow(topology.data)
    if (!stats.data) return laid
    const overlaid = laid.nodes.map((n) => {
      const s = stats.data[n.id]
      if (!s) return n
      return {
        ...n,
        data: {
          ...n.data,
          processor: {
            ...n.data.processor,
            stats: { processed: s.processed, errors: s.errors },
          },
        },
        selected: n.id === selected,
      }
    })
    return { nodes: overlaid, edges: laid.edges }
  }, [topology.data, stats.data, selected])

  // Close drawer when the selected processor vanishes from a refresh
  // (deleted out from under us).
  useEffect(() => {
    if (selected && topology.data && !topology.data.processors.some((p) => p.name === selected)) {
      setSelected(null)
    }
  }, [topology.data, selected])

  const onNodeClick: NodeMouseHandler = (_, node) => setSelected(node.id)
  const onPaneClick = () => setSelected(null)

  const selectedProcessor: Processor | null =
    topology.data?.processors.find((p) => p.name === selected) ?? null

  return (
    <div className="relative h-full w-full">
      <div className="absolute inset-0">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={true}
          fitView
          fitViewOptions={{ padding: 0.2 }}
          minZoom={0.2}
          maxZoom={2}
          onNodeClick={onNodeClick}
          onPaneClick={onPaneClick}
          proOptions={{ hideAttribution: true }}
        >
          <Background color="#222244" gap={24} />
          <Controls showInteractive={false} />
          <MiniMap
            nodeStrokeWidth={1}
            nodeColor={() => '#0f3460'}
            maskColor="rgba(15,15,26,0.6)"
          />
        </ReactFlow>
      </div>
      <div className="pointer-events-none absolute left-4 top-4 z-10">
        <h1 className="text-base font-semibold text-white">Graph</h1>
        {topology.data && (
          <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {topology.data.processors.length} processors ·{' '}
            {topology.data.processors.reduce(
              (a, p) => a + Object.values(p.connections ?? {}).reduce((b, xs) => b + xs.length, 0),
              0,
            )}{' '}
            connections · {topology.data.entryPoints.length} entry points
          </p>
        )}
        {topology.isError && <p className="text-[11px] text-red-400">failed to load /api/flow</p>}
      </div>
      {selectedProcessor && (
        <ProcessorDrawer
          processor={selectedProcessor}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  )
}

function ProcessorDrawer({ processor, onClose }: { processor: Processor; onClose: () => void }) {
  return (
    <aside
      className="absolute right-0 top-0 z-20 flex h-full w-[400px] flex-col"
      style={{ background: 'var(--surface)', borderLeft: '1px solid var(--border)' }}
    >
      <header
        className="flex items-center justify-between px-4 py-3"
        style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)' }}
      >
        <div>
          <div className="font-semibold text-white">{processor.name}</div>
          <div className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            {processor.type} · {processor.state}
          </div>
        </div>
        <button
          onClick={onClose}
          className="text-lg"
          style={{ color: 'var(--text-muted)' }}
          aria-label="close"
        >
          ×
        </button>
      </header>
      <div className="flex-1 overflow-y-auto p-4">
        <h3 className="mb-2 text-[11px] uppercase tracking-widest" style={{ color: 'var(--accent)' }}>
          config
        </h3>
        {processor.config && Object.keys(processor.config).length > 0 ? (
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
            {Object.entries(processor.config).map(([k, v]) => (
              <div key={k} className="contents">
                <dt style={{ color: 'var(--text-muted)' }}>{k}</dt>
                <dd className="break-all">{String(v ?? '')}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p style={{ color: 'var(--text-muted)' }}>no config keys</p>
        )}

        <h3 className="mb-2 mt-6 text-[11px] uppercase tracking-widest" style={{ color: 'var(--accent)' }}>
          outbound
        </h3>
        {processor.connections && Object.keys(processor.connections).length > 0 ? (
          <ul className="space-y-1">
            {Object.entries(processor.connections).flatMap(([rel, targets]) =>
              targets.map((t) => (
                <li key={`${rel}::${t}`}>
                  <span style={{ color: 'var(--text-muted)' }}>{rel}</span>
                  <span className="mx-2" style={{ color: 'var(--accent)' }}>
                    →
                  </span>
                  {t}
                </li>
              )),
            )}
          </ul>
        ) : (
          <p style={{ color: 'var(--text-muted)' }}>no outbound connections</p>
        )}

        <h3 className="mb-2 mt-6 text-[11px] uppercase tracking-widest" style={{ color: 'var(--accent)' }}>
          stats
        </h3>
        <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
          <dt style={{ color: 'var(--text-muted)' }}>processed</dt>
          <dd>{processor.stats?.processed ?? 0}</dd>
          <dt style={{ color: 'var(--text-muted)' }}>errors</dt>
          <dd>{processor.stats?.errors ?? 0}</dd>
        </dl>

        <p className="mt-6 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          edit drawer (inputs + save) lands in the next commit — this is the read-only MVP.
        </p>
      </div>
    </aside>
  )
}
