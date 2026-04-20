import { BaseEdge, EdgeLabelRenderer, getBezierPath, type EdgeProps } from '@xyflow/react'
import type { RelEdgeData } from '../lib/layout'

function colorFor(rel: string): string {
  if (rel === 'success') return 'var(--success)'
  if (rel === 'failure') return 'var(--error)'
  return 'var(--warning)'
}

// Motion dot count scales with log10 of the per-poll delta. Small
// flows get one dot; medium get two; heavy flows get three. Bigger
// counts would crowd the edge without communicating more.
function dotsForDelta(delta: number): number {
  if (delta <= 0) return 0
  if (delta < 10) return 1
  if (delta < 100) return 2
  return 3
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
  const motion = data?.motionDelta ?? 0
  const color = colorFor(rel)
  const dash = rel === 'failure' ? '6 3' : undefined

  const [path, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    curvature: 0.3,
  })

  const dots = dotsForDelta(motion)
  // Stagger dot start times across the path so they trail each other
  // instead of stacking. Duration ~1s per full traversal.
  const pathId = `${id}-path`

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        style={{
          stroke: color,
          strokeWidth: motion > 0 ? 2.5 : 2,
          strokeDasharray: dash,
          // Slight transparency so an edge that crosses behind a node
          // doesn't visually dominate. Nodes have opaque backgrounds
          // with a shadow on top — the combined effect reads as
          // "edge passes under node". Active edges get a tiny opacity
          // boost so they stand out against idle ones.
          opacity: motion > 0 ? 0.95 : 0.85,
          transition: 'stroke-width 0.3s, opacity 0.3s',
        }}
      />

      {dots > 0 && (
        <>
          <defs>
            <path id={pathId} d={path} fill="none" />
          </defs>
          {Array.from({ length: dots }).map((_, i) => (
            <circle key={i} r={3.5} fill={color} opacity={0.9}>
              <animateMotion
                dur="1.2s"
                repeatCount="indefinite"
                begin={`${(i * 1.2) / dots}s`}
              >
                <mpath href={`#${pathId}`} />
              </animateMotion>
            </circle>
          ))}
        </>
      )}

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
