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
import { ProcessorDrawer } from '../components/ProcessorDrawer'
import { AddProcessorDialog } from '../components/AddProcessorDialog'
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
  const [addOpen, setAddOpen] = useState(false)

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
      <div className="absolute left-4 right-4 top-4 z-10 flex items-start justify-between">
        <div className="pointer-events-none">
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
          {topology.isError && <p className="text-[11px]" style={{ color: 'var(--error)' }}>failed to load /api/flow</p>}
        </div>
        <button
          onClick={() => setAddOpen(true)}
          className="rounded px-3 py-1 text-[12px]"
          style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
          title="add processor to runtime graph"
        >
          + add processor
        </button>
      </div>

      {/* Empty-state CTA — shows when the flow has no processors. The
          canvas is behind this but invisible; operators land here
          rather than on a blank black rectangle. */}
      {topology.isSuccess && topology.data.processors.length === 0 && (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center">
          <div
            className="pointer-events-auto flex flex-col items-center gap-3 rounded-lg border px-8 py-6 text-center"
            style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}
          >
            <h2 className="text-sm font-semibold text-white">Your flow is empty</h2>
            <p className="max-w-sm text-[12px]" style={{ color: 'var(--text-muted)' }}>
              Add a processor to begin. Sources + sinks are available through the same
              dialog — pick the type that matches what the processor does (e.g. GetFile
              source, PutFile sink, UpdateAttribute transform).
            </p>
            <button
              onClick={() => setAddOpen(true)}
              className="rounded px-4 py-1.5 text-[12px]"
              style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
            >
              + add your first processor
            </button>
          </div>
        </div>
      )}

      <AddProcessorDialog
        open={addOpen}
        existingNames={topology.data?.processors.map((p) => p.name) ?? []}
        onClose={() => setAddOpen(false)}
      />
      {selectedProcessor && topology.data && (
        <ProcessorDrawer
          processor={selectedProcessor}
          allProcessorNames={topology.data.processors.map((p) => p.name)}
          entryPoints={topology.data.entryPoints}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  )
}

