import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { Node } from '@xyflow/react'
import type { ProcessorNodeData } from '../lib/layout'

function formatNum(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

export function ProcessorNode({ data, selected }: NodeProps<Node<ProcessorNodeData>>) {
  const { processor, isEntry, isSink } = data
  const processed = processor.stats?.processed ?? 0
  const errors = processor.stats?.errors ?? 0
  const hasErrors = errors > 0

  const fill = hasErrors
    ? 'rgba(231, 76, 60, 0.10)'
    : processor.state === 'ENABLED'
      ? 'var(--surface-2)'
      : 'rgba(26, 26, 46, 0.6)'

  const borderColor = selected
    ? 'var(--selected)'
    : hasErrors
      ? 'var(--error)'
      : isEntry
        ? 'var(--entry)'
        : processor.state === 'ENABLED'
          ? '#0f3460'
          : '#333'

  return (
    <div
      className="relative rounded-lg px-3 py-2"
      style={{
        width: 220,
        minHeight: 88,
        background: fill,
        border: `2px solid ${borderColor}`,
        // Opaque surface ensures edges passing behind the node don't
        // bleed through. Selected nodes sit above all edges; unselected
        // nodes still sit above them (z-index default on React Flow
        // nodes is low enough that we only need a small bump here).
        boxShadow: selected
          ? '0 0 12px var(--selected), 0 2px 8px rgba(0,0,0,0.6)'
          : '0 2px 8px rgba(0,0,0,0.6)',
        transition: 'filter 0.15s, box-shadow 0.15s',
        cursor: 'grab',
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{
          width: 10,
          height: 10,
          background: 'var(--accent)',
          border: '2px solid var(--surface)',
        }}
      />
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <div className="truncate text-[13px] font-semibold text-white" title={processor.name}>
            {processor.name}
          </div>
          <div className="truncate text-[10px]" style={{ color: 'var(--text-muted)' }} title={processor.type}>
            {processor.type}
          </div>
        </div>
        {isEntry && (
          <span
            className="rounded-sm px-1 text-[9px] font-semibold"
            style={{ color: 'var(--entry)' }}
          >
            ENTRY
          </span>
        )}
        {!isEntry && isSink && (
          <span
            className="rounded-sm px-1 text-[9px] font-semibold"
            style={{ color: 'var(--warning)' }}
          >
            SINK
          </span>
        )}
      </div>
      <div className="mt-1 text-[11px]">
        <span style={{ color: 'var(--success)' }}>{formatNum(processed)}</span>
        <span style={{ color: '#555' }}> processed</span>
        {hasErrors && (
          <>
            <span style={{ color: '#555' }}> · </span>
            <span style={{ color: 'var(--error)' }}>{formatNum(errors)}</span>
            <span style={{ color: 'var(--error)' }}> errors</span>
          </>
        )}
      </div>
      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          width: 10,
          height: 10,
          background: 'var(--success)',
          border: '2px solid var(--surface)',
        }}
      />
    </div>
  )
}
