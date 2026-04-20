import { useCallback, useEffect, useRef, useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useReactFlow,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type NodeMouseHandler,
  type EdgeMouseHandler,
  type Connection,
} from '@xyflow/react'
import { api } from '../api/client'
import { layoutFlow, type ProcessorNodeData, type RelEdgeData, NODE_W, NODE_H } from '../lib/layout'
import { layoutStore } from '../lib/layoutStore'
import { ProcessorNode } from '../components/ProcessorNode'
import { RelationshipEdge } from '../components/RelationshipEdge'
import { ProcessorDrawer } from '../components/ProcessorDrawer'
import { EdgeDrawer, type EdgeSelection } from '../components/EdgeDrawer'
import { SourcesPanel } from '../components/SourcesPanel'
import { TestFlowFileDialog } from '../components/TestFlowFileDialog'
import { ProcessorPalette, PALETTE_MIME } from '../components/ProcessorPalette'
import { EdgeRelationshipPicker } from '../components/EdgeRelationshipPicker'
import { useAddConnection, useAddProcessor, useAddSource, useReloadFlow } from '../lib/mutations'
import type { Processor, RegistryEntry } from '../api/types'

const nodeTypes = { processor: ProcessorNode }
const edgeTypes = { relationship: RelationshipEdge }

// Topology poll (slow): 15 s. Stats poll (fast): 2 s.
// Topology changes re-run dagre fallback; saved layoutStore positions win
// when present. Stats poll mutates node.data in place.
const TOPOLOGY_POLL_MS = 15_000
const STATS_POLL_MS = 2_000

export function GraphPage() {
  return (
    <ReactFlowProvider>
      <GraphPageInner />
    </ReactFlowProvider>
  )
}

function GraphPageInner() {
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
  const registry = useQuery<RegistryEntry[]>({
    queryKey: ['registry'],
    queryFn: api.registry,
    staleTime: 5 * 60_000,
  })

  const [selected, setSelected] = useState<string | null>(null)
  const [selectedEdge, setSelectedEdge] = useState<EdgeSelection | null>(null)
  const [sourcesOpen, setSourcesOpen] = useState(false)
  const [testOpen, setTestOpen] = useState(false)
  const [reloadMsg, setReloadMsg] = useState<string | null>(null)
  const [paletteCollapsed, setPaletteCollapsed] = useState(false)
  const [pendingEdge, setPendingEdge] = useState<{ conn: Connection; anchor: { x: number; y: number } } | null>(null)

  const reload = useReloadFlow()
  const addConn = useAddConnection()
  const addProc = useAddProcessor()
  const addSrc = useAddSource()
  const rf = useReactFlow()
  const paneRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!reloadMsg) return
    const t = setTimeout(() => setReloadMsg(null), 4000)
    return () => clearTimeout(t)
  }, [reloadMsg])

  // Prune layoutStore entries for processors no longer in the flow.
  useEffect(() => {
    if (topology.data) {
      layoutStore.prune(new Set(topology.data.processors.map((p) => p.name)))
    }
  }, [topology.data])

  const onReload = async () => {
    try {
      const body = await reload.mutateAsync()
      if (body['error']) setReloadMsg(`error: ${body['error']}`)
      else {
        const parts: string[] = []
        if (typeof body['added'] === 'number') parts.push(`+${body['added']}`)
        if (typeof body['removed'] === 'number') parts.push(`-${body['removed']}`)
        if (typeof body['updated'] === 'number') parts.push(`~${body['updated']}`)
        setReloadMsg(`reloaded ${parts.join(' ')}`.trim())
      }
    } catch (e) {
      setReloadMsg(`error: ${(e as Error).message}`)
    }
  }

  // Edge drawing: instead of committing "success" immediately, anchor the
  // relationship picker near the drop point so the user can pick the
  // right relationship for the source processor (success / failure /
  // unmatched / custom).
  const onConnect = useCallback((conn: Connection) => {
    if (!conn.source || !conn.target) return
    const anchor = { x: (window.event as MouseEvent | undefined)?.clientX ?? 200,
                     y: (window.event as MouseEvent | undefined)?.clientY ?? 200 }
    setPendingEdge({ conn, anchor })
  }, [])

  const commitPendingEdge = async (relationship: string) => {
    const pending = pendingEdge
    if (!pending) return
    setPendingEdge(null)
    try {
      await addConn.mutateAsync({ from: pending.conn.source!, relationship, to: pending.conn.target! })
    } catch {
      /* mutation surfaces its own error */
    }
  }

  // Relationship suggestions: source processor's TypeInfo.relationships
  // from the registry, filtered to ones that aren't already connected
  // to this target (avoid duplicates).
  const edgeSuggestions = useMemo(() => {
    if (!pendingEdge || !registry.data || !topology.data) return ['success']
    const src = topology.data.processors.find((p) => p.name === pendingEdge.conn.source)
    const entry = registry.data.find((e) => e.name === src?.type)
    const known = new Set<string>(['success', 'failure', ...(entry ? ((entry as unknown as { relationships?: string[] }).relationships ?? []) : [])])
    const existing = src?.connections ?? {}
    for (const [rel, targets] of Object.entries(existing)) {
      if ((targets ?? []).includes(pendingEdge.conn.target!)) known.delete(rel)
    }
    return Array.from(known)
  }, [pendingEdge, registry.data, topology.data])

  // Drag-and-drop from palette. On drop:
  //   1. read processor type from MIME payload
  //   2. translate screen → flow coordinates via React Flow
  //   3. persist the position before POSTing so the node lands where
  //      the user released it, not dagre's grid slot
  //   4. POST /api/processors/add with an auto-generated unique name
  // Only accept drops when the palette MIME is present. Unconditionally
  // calling preventDefault() interferes with React Flow's own pointer-based
  // node-drag: the pane starts accepting an HTML5 drop, which confuses
  // React Flow into cancelling the drag mid-move. See the "node gets
  // stuck" symptom that appeared after wiring the palette.
  const onDragOver = useCallback((e: React.DragEvent) => {
    if (!e.dataTransfer.types.includes(PALETTE_MIME)) return
    e.preventDefault()
    e.dataTransfer.dropEffect = 'copy'
  }, [])

  const onDrop = useCallback(async (e: React.DragEvent) => {
    const type = e.dataTransfer.getData(PALETTE_MIME)
    if (!type) return
    e.preventDefault()
    const bounds = paneRef.current?.getBoundingClientRect()
    if (!bounds) return
    const position = rf.screenToFlowPosition({
      x: e.clientX - NODE_W / 2,
      y: e.clientY - NODE_H / 2,
    })

    const existingNames = new Set((topology.data?.processors ?? []).map((p) => p.name))
    const name = uniqueName(type, existingNames)

    // Persist position first; the flow refresh triggered by the add
    // mutation picks it up in layoutFlow. Branch on registry kind:
    // processor → /api/processors/add, source → /api/sources/add.
    layoutStore.set(name, position)

    const entry = (registry.data ?? []).find((r) => r.name === type)
    const isSource = entry?.kind === 'source'

    try {
      if (isSource) {
        await addSrc.mutateAsync({ name, type, config: {} })
      } else {
        await addProc.mutateAsync({ name, type, config: {}, connections: {} })
      }
    } catch (err) {
      layoutStore.delete(name)
      setReloadMsg(`add failed: ${(err as Error).message}`)
    }
  }, [addProc, addSrc, rf, topology.data, registry.data])

  const onNodeDragStop = useCallback((_: unknown, node: Node<ProcessorNodeData>) => {
    layoutStore.set(node.id, { x: node.position.x, y: node.position.y })
  }, [])

  // React Flow controlled state. useNodesState / useEdgesState give us the
  // onNodesChange / onEdgesChange handlers React Flow needs to commit
  // drag-time position updates. Without these, setting nodesDraggable=true
  // gives the appearance of being stuck (node only visibly moves after a
  // full topology re-layout).
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<ProcessorNodeData>>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge<RelEdgeData>>([])

  // Re-layout on topology change. We preserve user-dragged positions
  // via the layoutStore (which layoutFlow reads); dagre is the fallback
  // for processors without a saved position.
  useEffect(() => {
    if (!topology.data) return
    const laid = layoutFlow(topology.data)
    setNodes(laid.nodes)
    setEdges(laid.edges)
  }, [topology.data, setNodes, setEdges])

  // Stats overlay — patch node.data.processor.stats in place so React
  // Flow doesn't re-layout on every 2s stats tick.
  useEffect(() => {
    if (!stats.data) return
    setNodes((prev) =>
      prev.map((n) => {
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
        }
      }),
    )
  }, [stats.data, setNodes])

  // Reflect drawer selection on the node (for the selected-ring styling)
  // without forcing a re-layout.
  useEffect(() => {
    setNodes((prev) => prev.map((n) => (n.selected === (n.id === selected) ? n : { ...n, selected: n.id === selected })))
  }, [selected, setNodes])

  useEffect(() => {
    if (selected && topology.data && !topology.data.processors.some((p) => p.name === selected)) {
      setSelected(null)
    }
  }, [topology.data, selected])

  useEffect(() => {
    if (!selectedEdge || !topology.data) return
    const src = topology.data.processors.find((p) => p.name === selectedEdge.from)
    const targets = src?.connections?.[selectedEdge.relationship] ?? []
    if (!targets.includes(selectedEdge.to)) setSelectedEdge(null)
  }, [topology.data, selectedEdge])

  const onNodeClick: NodeMouseHandler = (_, node) => {
    setSelectedEdge(null)
    setSelected(node.id)
  }
  const onEdgeClick: EdgeMouseHandler = (_, edge) => {
    setSelected(null)
    const rel = (edge.data as RelEdgeData | undefined)?.relationship ?? 'success'
    setSelectedEdge({ from: edge.source, relationship: rel, to: edge.target })
  }
  const onPaneClick = () => {
    setSelected(null)
    setSelectedEdge(null)
  }

  const selectedProcessor: Processor | null =
    topology.data?.processors.find((p) => p.name === selected) ?? null

  return (
    <div className="flex h-full w-full">
      <ProcessorPalette collapsed={paletteCollapsed} onToggle={() => setPaletteCollapsed((c) => !c)} />
      <div className="relative flex-1">
        <div className="absolute inset-0" ref={paneRef} onDragOver={onDragOver} onDrop={onDrop}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            nodesDraggable={true}
            nodesConnectable={true}
            elementsSelectable={true}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeDragStop={onNodeDragStop}
            fitView
            fitViewOptions={{ padding: 0.2 }}
            minZoom={0.2}
            maxZoom={2}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
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
        <div className="pointer-events-none absolute left-4 right-4 top-4 z-10 flex items-start justify-between">
          <div>
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
          <div className="pointer-events-auto flex items-center gap-2">
            {reloadMsg && (
              <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
                {reloadMsg}
              </span>
            )}
            <button
              onClick={onReload}
              disabled={reload.isPending}
              className="rounded border px-3 py-1 text-[12px]"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
              title="re-parse config.yaml and diff against the running flow"
            >
              {reload.isPending ? 'reloading…' : 'reload'}
            </button>
            <button
              onClick={() => setSourcesOpen(true)}
              className="rounded border px-3 py-1 text-[12px]"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
              title="start/stop connector sources"
            >
              sources
            </button>
            <button
              onClick={() => setTestOpen(true)}
              className="rounded border px-3 py-1 text-[12px]"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
              title="push a synthetic FlowFile into the graph"
            >
              test flowfile
            </button>
          </div>
        </div>

        {topology.isSuccess && topology.data.processors.length === 0 && (
          <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center">
            <div
              className="flex flex-col items-center gap-2 rounded-lg border px-8 py-6 text-center"
              style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}
            >
              <h2 className="text-sm font-semibold text-white">Your flow is empty</h2>
              <p className="max-w-sm text-[12px]" style={{ color: 'var(--text-muted)' }}>
                Drag a processor from the palette on the left onto the canvas to begin.
              </p>
            </div>
          </div>
        )}

        {selectedProcessor && topology.data && (
          <ProcessorDrawer
            processor={selectedProcessor}
            allProcessorNames={topology.data.processors.map((p) => p.name)}
            entryPoints={topology.data.entryPoints}
            onClose={() => setSelected(null)}
          />
        )}
        {selectedEdge && (
          <EdgeDrawer edge={selectedEdge} onClose={() => setSelectedEdge(null)} />
        )}
        {sourcesOpen && <SourcesPanel onClose={() => setSourcesOpen(false)} />}
        <TestFlowFileDialog open={testOpen} flow={topology.data} onClose={() => setTestOpen(false)} />
        <EdgeRelationshipPicker
          anchor={pendingEdge?.anchor ?? null}
          suggestions={edgeSuggestions}
          onPick={commitPendingEdge}
          onCancel={() => setPendingEdge(null)}
        />
      </div>
    </div>
  )
}

// Auto-generate a unique processor name from its type. NiFi names things
// like "UpdateAttribute", "UpdateAttribute 2" — we do the same so users
// can drop multiple of the same type without naming every one.
function uniqueName(type: string, existing: Set<string>): string {
  if (!existing.has(type)) return type
  for (let i = 2; i < 10_000; i++) {
    const candidate = `${type} ${i}`
    if (!existing.has(candidate)) return candidate
  }
  return `${type}-${Date.now()}`
}
