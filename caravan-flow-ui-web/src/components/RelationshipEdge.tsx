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
              padding: '2px 7px',
              borderRadius: 10,
              border: `1px solid ${color}`,
              color,
              fontSize: 10,
              fontWeight: 600,
              letterSpacing: '0.05em',
              pointerEvents: 'none',
              whiteSpace: 'nowrap',
              boxShadow: '0 1px 3px rgba(0,0,0,0.5)',
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
