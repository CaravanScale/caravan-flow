import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from '@xyflow/react'
import type { RelEdgeData } from '../lib/layout'

function colorFor(rel: string): string {
  if (rel === 'success') return 'var(--success)'
  if (rel === 'failure') return 'var(--error)'
  return 'var(--warning)'
}

export function RelationshipEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  markerEnd,
}: EdgeProps & { data?: RelEdgeData }) {
  const rel = data?.relationship ?? 'success'
  const color = colorFor(rel)
  const dash = rel === 'failure' ? '6 3' : undefined

  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 10,
  })

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke: color,
          strokeWidth: 2,
          strokeDasharray: dash,
        }}
      />
      {rel !== 'success' && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              background: 'var(--surface)',
              padding: '1px 6px',
              borderRadius: 3,
              color: 'var(--text-muted)',
              fontSize: 10,
              pointerEvents: 'none',
            }}
            className="nodrag nopan"
          >
            {rel}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}
