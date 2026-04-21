import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'
import { ExpressionBuilder } from '../ExpressionBuilder'

// RouteRecord config:
//   routes = "name: expr; name2: expr2"
// Each expression is a record-level predicate (e.g. `tier == "gold"`).
// First-match wins; non-matching records flow to 'unmatched'.
//
// Shape mirrors the generic KeyValueList + Expression combo, but framed
// as routing rules so the operator reads "when X, route to Y" instead of
// a raw key/value pair. Click ✎ on a row to assemble the expression in
// the full builder modal.

interface Route {
  name: string
  expression: string
}

function parse(raw: string): Route[] {
  const out: Route[] = []
  if (!raw) return out
  for (const chunk of raw.split(';')) {
    const s = chunk.trim()
    if (!s) continue
    const colon = s.indexOf(':')
    if (colon < 0) { out.push({ name: s, expression: '' }); continue }
    out.push({
      name: s.slice(0, colon).trim(),
      expression: s.slice(colon + 1).trim(),
    })
  }
  return out
}

function serialize(routes: Route[]): string {
  return routes
    .filter((r) => r.name || r.expression)
    .map((r) => `${r.name}:${r.expression}`)
    .join(';')
}

export function RouteRecordWizard({ values, onChange, processorName }: WizardProps) {
  const initial = values.routes ?? ''
  const [routes, setRoutes] = useState<Route[]>(() => parse(initial))
  const [builderTarget, setBuilderTarget] = useState<{ initial: string; save: (v: string) => void } | null>(null)

  useEffect(() => {
    setRoutes(parse(initial))
  }, [initial])

  const push = (next: Route[]) => {
    setRoutes(next)
    onChange({ ...values, routes: serialize(next) })
  }
  const add = () => push([...routes, { name: '', expression: '' }])
  const remove = (i: number) => push(routes.filter((_, j) => j !== i))
  const patch = (i: number, key: keyof Route, v: string) => {
    const next = routes.slice()
    next[i] = { ...next[i], [key]: v }
    push(next)
  }

  const serialized = useMemo(() => serialize(routes), [routes])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          routes
        </h4>
        <button
          onClick={add}
          className="rounded border px-2 py-0.5 text-[11px]"
          style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--accent)' }}
        >
          + route
        </button>
      </div>

      {routes.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No routes yet. Click <strong>+ route</strong> to add one.
        </p>
      )}

      <div className="flex flex-col gap-2">
        {routes.map((r, i) => (
          <div
            key={i}
            className="flex flex-col gap-2 rounded border p-2"
            style={{ borderColor: 'var(--border)', background: '#0a0a14' }}
          >
            <div className="flex items-end gap-2">
              <label className="flex min-w-0 flex-1 flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>route name</span>
                <input
                  value={r.name}
                  onChange={(e) => patch(i, 'name', e.target.value)}
                  placeholder="premium"
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
              <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>when (expression)</span>
              <textarea
                value={r.expression}
                onChange={(e) => patch(i, 'expression', e.target.value)}
                placeholder='tier == "gold"'
                rows={2}
                className="rounded border px-2 py-1 font-mono text-[12px]"
                style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
              />
            </label>
          </div>
        ))}
      </div>

      <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
        First-match wins. Records matching none of the above flow to <code>unmatched</code>.
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
