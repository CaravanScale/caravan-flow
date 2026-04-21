import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'
import { ExpressionBuilder } from '../ExpressionBuilder'

// Shared shape for EvaluateExpression ("expressions" param) and
// UpdateRecord ("updates" param): a KeyValueList of name → expression.
// Both read "Later rows see values of earlier rows." Purpose-built
// wizard renames "key" / "value" to the domain nouns so the row reads
// like "compute total = amount * 1.07" instead of a raw k/v pair.

interface Row {
  name: string
  expression: string
}

function parse(raw: string): Row[] {
  const out: Row[] = []
  if (!raw) return out
  for (const chunk of raw.split(';')) {
    const s = chunk.trim()
    if (!s) continue
    const eq = s.indexOf('=')
    if (eq < 0) { out.push({ name: s, expression: '' }); continue }
    out.push({
      name: s.slice(0, eq).trim(),
      expression: s.slice(eq + 1).trim(),
    })
  }
  return out
}

function serialize(rows: Row[]): string {
  return rows
    .filter((r) => r.name || r.expression)
    .map((r) => `${r.name}=${r.expression}`)
    .join(';')
}

interface BaseProps extends WizardProps {
  paramName: string
  targetLabel: string // "attribute" or "field"
}

function ComputeWizardBase({ paramName, targetLabel, values, onChange, processorName }: BaseProps) {
  const initial = values[paramName] ?? ''
  const [rows, setRows] = useState<Row[]>(() => parse(initial))
  const [builderTarget, setBuilderTarget] = useState<{ initial: string; save: (v: string) => void } | null>(null)

  useEffect(() => {
    setRows(parse(initial))
  }, [initial])

  const push = (next: Row[]) => {
    setRows(next)
    onChange({ ...values, [paramName]: serialize(next) })
  }
  const add = () => push([...rows, { name: '', expression: '' }])
  const remove = (i: number) => push(rows.filter((_, j) => j !== i))
  const patch = (i: number, key: keyof Row, v: string) => {
    const next = rows.slice()
    next[i] = { ...next[i], [key]: v }
    push(next)
  }

  const serialized = useMemo(() => serialize(rows), [rows])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          {paramName}
        </h4>
        <button
          onClick={add}
          className="rounded border px-2 py-0.5 text-[11px]"
          style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--accent)' }}
        >
          + row
        </button>
      </div>

      {rows.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No rows yet. Click <strong>+ row</strong> to compute a new {targetLabel}.
        </p>
      )}

      <div className="flex flex-col gap-2">
        {rows.map((r, i) => (
          <div
            key={i}
            className="flex flex-col gap-2 rounded border p-2"
            style={{ borderColor: 'var(--border)', background: '#0a0a14' }}
          >
            <div className="flex items-end gap-2">
              <label className="flex min-w-0 flex-1 flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>{targetLabel}</span>
                <input
                  value={r.name}
                  onChange={(e) => patch(i, 'name', e.target.value)}
                  placeholder={targetLabel === 'attribute' ? 'tax' : 'total'}
                  className="rounded border px-2 py-1 text-[12px]"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
                />
              </label>
              <button
                onClick={() => setBuilderTarget({
                  initial: r.expression,
                  save: (v) => patch(i, 'expression', v),
                })}
                className="rounded border px-1.5 py-1 text-[10px]"
                style={{ background: 'transparent', borderColor: 'var(--accent)', color: 'var(--accent)' }}
                title="open expression builder"
                type="button"
              >
                ✎ builder
              </button>
              <button
                onClick={() => remove(i)}
                className="px-2 py-1 text-[14px]"
                style={{ color: 'var(--error)' }}
                title="remove"
                type="button"
              >×</button>
            </div>
            <label className="flex flex-col">
              <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>= expression</span>
              <textarea
                value={r.expression}
                onChange={(e) => patch(i, 'expression', e.target.value)}
                placeholder="amount * 0.07"
                rows={2}
                className="rounded border px-2 py-1 font-mono text-[12px]"
                style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
              />
            </label>
          </div>
        ))}
      </div>

      <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
        Later rows can reference earlier {targetLabel}s.
      </p>

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

      {builderTarget && (
        <ExpressionBuilder
          open
          initialValue={builderTarget.initial}
          processorName={processorName}
          onSave={(v) => { builderTarget.save(v); setBuilderTarget(null) }}
          onCancel={() => setBuilderTarget(null)}
        />
      )}
    </div>
  )
}

export function EvaluateExpressionWizard(props: WizardProps) {
  return <ComputeWizardBase {...props} paramName="expressions" targetLabel="attribute" />
}

export function UpdateRecordWizard(props: WizardProps) {
  return <ComputeWizardBase {...props} paramName="updates" targetLabel="field" />
}
