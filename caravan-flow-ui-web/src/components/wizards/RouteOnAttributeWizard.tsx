import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'

// RouteOnAttribute DSL:
//   name: attr OP value; name2: attr2 OP2 value2
// OP ∈ {EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT}
// EXISTS takes no value.

type RouteOp =
  | 'EQ'
  | 'NEQ'
  | 'CONTAINS'
  | 'STARTSWITH'
  | 'ENDSWITH'
  | 'EXISTS'
  | 'GT'
  | 'LT'

const OPS: RouteOp[] = ['EQ', 'NEQ', 'CONTAINS', 'STARTSWITH', 'ENDSWITH', 'EXISTS', 'GT', 'LT']

interface Rule {
  name: string
  attr: string
  op: RouteOp
  value: string
}

function parse(raw: string): Rule[] {
  const out: Rule[] = []
  if (!raw) return out
  for (const chunk of raw.split(';')) {
    const s = chunk.trim()
    if (!s) continue
    const colon = s.indexOf(':')
    if (colon < 0) continue
    const name = s.slice(0, colon).trim()
    const rest = s.slice(colon + 1).trim().split(/\s+/)
    const attr = rest[0] ?? ''
    const op = ((rest[1] ?? 'EQ').toUpperCase() as RouteOp)
    const value = rest.slice(2).join(' ')
    out.push({ name, attr, op: OPS.includes(op) ? op : 'EQ', value })
  }
  return out
}

function serialize(rules: Rule[]): string {
  return rules
    .map((r) => {
      const tail = r.op === 'EXISTS' ? r.op : `${r.op} ${r.value}`.trim()
      return `${r.name}: ${r.attr} ${tail}`.trim()
    })
    .join('; ')
}

export function RouteOnAttributeWizard({ values, onChange }: WizardProps) {
  const initial = values.routes ?? ''
  const [rules, setRules] = useState<Rule[]>(() => parse(initial))

  useEffect(() => {
    setRules(parse(initial))
  }, [initial])

  const push = (next: Rule[]) => {
    setRules(next)
    onChange({ ...values, routes: serialize(next) })
  }

  const add = () => push([...rules, { name: '', attr: '', op: 'EQ', value: '' }])
  const remove = (i: number) => push(rules.filter((_, j) => j !== i))
  const patch = (i: number, key: keyof Rule, v: string) => {
    const next = rules.slice()
    next[i] = { ...next[i], [key]: v }
    push(next)
  }

  const serialized = useMemo(() => serialize(rules), [rules])

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

      {rules.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No routes yet. Click <strong>+ route</strong> to add one.
        </p>
      )}

      <div className="flex flex-col gap-2">
        {rules.map((r, i) => (
          <div
            key={i}
            className="rounded border p-2"
            style={{ borderColor: 'var(--border)', background: '#0a0a14' }}
          >
            <div className="grid grid-cols-[1fr_1fr_auto_1fr_auto] items-end gap-2">
              <label className="flex flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>name</span>
                <input
                  value={r.name}
                  onChange={(e) => patch(i, 'name', e.target.value)}
                  placeholder="premium"
                  className="rounded border px-2 py-1 text-[12px]"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
                />
              </label>
              <label className="flex flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>attribute</span>
                <input
                  value={r.attr}
                  onChange={(e) => patch(i, 'attr', e.target.value)}
                  placeholder="tier"
                  className="rounded border px-2 py-1 text-[12px]"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
                />
              </label>
              <label className="flex flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>op</span>
                <select
                  value={r.op}
                  onChange={(e) => patch(i, 'op', e.target.value)}
                  className="rounded border px-1 py-1 text-[12px]"
                  style={{ background: 'var(--surface-2)', borderColor: 'var(--border)', color: 'var(--text)' }}
                >
                  {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                </select>
              </label>
              <label className="flex flex-col">
                <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>value</span>
                <input
                  value={r.value}
                  disabled={r.op === 'EXISTS'}
                  onChange={(e) => patch(i, 'value', e.target.value)}
                  placeholder={r.op === 'EXISTS' ? '(n/a)' : 'premium'}
                  className="rounded border px-2 py-1 text-[12px]"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
                />
              </label>
              <button
                onClick={() => remove(i)}
                className="px-2 py-1 text-[12px]"
                style={{ color: 'var(--error)' }}
                title="remove"
              >×</button>
            </div>
          </div>
        ))}
      </div>

      <details className="mt-2">
        <summary className="cursor-pointer text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          raw dsl
        </summary>
        <pre
          className="mt-1 overflow-auto rounded p-2 text-[11px]"
          style={{ background: '#0a0a14', border: '1px solid var(--border)', color: 'var(--text-muted)' }}
        >
          {serialized || '(empty)'}
        </pre>
      </details>
    </div>
  )
}
