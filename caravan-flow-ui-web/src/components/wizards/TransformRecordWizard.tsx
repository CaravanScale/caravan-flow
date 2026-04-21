import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'

// TransformRecord DSL:
//   rename:old:new
//   remove:field
//   add:field:value
//   copy:from:to
//   toUpper:field
//   toLower:field
//   default:field:value
//   compute:target:expression
// Directives separated by ';'. Wizard edits the list as rows and
// serializes back to this exact string — so the operator can flip
// between wizard and raw with no loss.

type OpKind =
  | 'rename'
  | 'remove'
  | 'add'
  | 'copy'
  | 'toUpper'
  | 'toLower'
  | 'default'
  | 'compute'

interface Op {
  kind: OpKind
  // Position-indexed args. Arity varies per kind; renderer reads by index.
  args: string[]
}

const OP_SPECS: Record<OpKind, { arity: number; labels: string[] }> = {
  rename: { arity: 2, labels: ['from field', 'to field'] },
  remove: { arity: 1, labels: ['field'] },
  add: { arity: 2, labels: ['field', 'value'] },
  copy: { arity: 2, labels: ['from field', 'to field'] },
  toUpper: { arity: 1, labels: ['field'] },
  toLower: { arity: 1, labels: ['field'] },
  default: { arity: 2, labels: ['field', 'default value'] },
  compute: { arity: 2, labels: ['target field', 'expression'] },
}

function parse(raw: string): Op[] {
  const ops: Op[] = []
  if (!raw) return ops
  for (const chunk of raw.split(';')) {
    const s = chunk.trim()
    if (!s) continue
    const parts = s.split(':').map((p) => p.trim())
    const kind = parts[0] as OpKind
    if (!(kind in OP_SPECS)) continue
    ops.push({ kind, args: parts.slice(1) })
  }
  return ops
}

function serialize(ops: Op[]): string {
  return ops
    .map((o) => [o.kind, ...o.args.slice(0, OP_SPECS[o.kind].arity)].join(':'))
    .join('; ')
}

export function TransformRecordWizard({ values, onChange }: WizardProps) {
  const initial = values.operations ?? ''
  const [ops, setOps] = useState<Op[]>(() => parse(initial))

  // When the parent reloads (e.g. upstream discard), re-parse.
  useEffect(() => {
    setOps(parse(initial))
  }, [initial])

  // Bubble every edit up so the drawer's Save picks it up.
  const push = (next: Op[]) => {
    setOps(next)
    onChange({ ...values, operations: serialize(next) })
  }

  const addRow = () => push([...ops, { kind: 'rename', args: ['', ''] }])
  const removeRow = (i: number) => push(ops.filter((_, j) => j !== i))
  const setKind = (i: number, kind: OpKind) => {
    const next = ops.slice()
    const prev = next[i]
    // Preserve existing args up to the new arity, pad with empty strings.
    const arity = OP_SPECS[kind].arity
    const args = Array.from({ length: arity }, (_, j) => prev.args[j] ?? '')
    next[i] = { kind, args }
    push(next)
  }
  const setArg = (i: number, argIdx: number, v: string) => {
    const next = ops.slice()
    const args = next[i].args.slice()
    args[argIdx] = v
    next[i] = { ...next[i], args }
    push(next)
  }
  const moveRow = (i: number, dir: -1 | 1) => {
    const j = i + dir
    if (j < 0 || j >= ops.length) return
    const next = ops.slice()
    ;[next[i], next[j]] = [next[j], next[i]]
    push(next)
  }

  const serialized = useMemo(() => serialize(ops), [ops])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          operations
        </h4>
        <button
          onClick={addRow}
          className="rounded border px-2 py-0.5 text-[11px]"
          style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--accent)' }}
        >
          + op
        </button>
      </div>

      {ops.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No operations yet. Click <strong>+ op</strong> to add one.
        </p>
      )}

      <div className="flex flex-col gap-2">
        {ops.map((op, i) => {
          const spec = OP_SPECS[op.kind]
          return (
            <div
              key={i}
              className="rounded border p-2"
              style={{ borderColor: 'var(--border)', background: '#0a0a14' }}
            >
              <div className="flex items-center gap-2">
                <select
                  value={op.kind}
                  onChange={(e) => setKind(i, e.target.value as OpKind)}
                  className="rounded border px-1 py-0.5 text-[11px]"
                  style={{ background: 'var(--surface-2)', borderColor: 'var(--border)', color: 'var(--text)' }}
                >
                  {(Object.keys(OP_SPECS) as OpKind[]).map((k) => (
                    <option key={k} value={k}>{k}</option>
                  ))}
                </select>
                <div className="flex-1" />
                <button
                  onClick={() => moveRow(i, -1)}
                  disabled={i === 0}
                  className="px-1 text-[11px]"
                  style={{ color: 'var(--text-muted)' }}
                  title="move up"
                >↑</button>
                <button
                  onClick={() => moveRow(i, 1)}
                  disabled={i === ops.length - 1}
                  className="px-1 text-[11px]"
                  style={{ color: 'var(--text-muted)' }}
                  title="move down"
                >↓</button>
                <button
                  onClick={() => removeRow(i)}
                  className="px-1 text-[11px]"
                  style={{ color: 'var(--error)' }}
                  title="remove"
                >×</button>
              </div>
              <div className="mt-2 grid grid-cols-2 gap-2">
                {spec.labels.map((label, j) => (
                  <label key={j} className="flex flex-col">
                    <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
                      {label}
                    </span>
                    <input
                      value={op.args[j] ?? ''}
                      onChange={(e) => setArg(i, j, e.target.value)}
                      className="rounded border px-2 py-1 text-[12px]"
                      style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
                    />
                  </label>
                ))}
              </div>
            </div>
          )
        })}
      </div>

      <details className="mt-2">
        <summary className="cursor-pointer text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          raw dsl
        </summary>
        <pre
          className="mt-1 overflow-auto rounded p-2 text-[11px]"
          style={{ background: '#0a0a14', border: '1px solid var(--border)', color: 'var(--text)' }}
        >
          {serialized || '(empty)'}
        </pre>
      </details>
    </div>
  )
}
