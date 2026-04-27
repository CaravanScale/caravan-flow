import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'

// ExtractRecordField config:
//   fields = "fieldPath:attrName;fieldPath2:attrName2"
//   recordIndex = integer, default 0

interface Pair {
  path: string
  attr: string
}

function parse(raw: string): Pair[] {
  const out: Pair[] = []
  if (!raw) return out
  for (const chunk of raw.split(';')) {
    const s = chunk.trim()
    if (!s) continue
    const colon = s.indexOf(':')
    if (colon < 0) { out.push({ path: s, attr: '' }); continue }
    out.push({
      path: s.slice(0, colon).trim(),
      attr: s.slice(colon + 1).trim(),
    })
  }
  return out
}

function serialize(pairs: Pair[]): string {
  return pairs
    .filter((p) => p.path || p.attr)
    .map((p) => `${p.path}:${p.attr || lastSegment(p.path)}`)
    .join(';')
}

function lastSegment(path: string): string {
  const parts = path.split('.')
  return parts[parts.length - 1] ?? ''
}

export function ExtractRecordFieldWizard({ values, onChange }: WizardProps) {
  const initial = values.fields ?? ''
  const [pairs, setPairs] = useState<Pair[]>(() => parse(initial))

  useEffect(() => {
    setPairs(parse(initial))
  }, [initial])

  const push = (next: Pair[]) => {
    setPairs(next)
    onChange({ ...values, fields: serialize(next) })
  }
  const add = () => push([...pairs, { path: '', attr: '' }])
  const remove = (i: number) => push(pairs.filter((_, j) => j !== i))
  const patch = (i: number, key: keyof Pair, v: string) => {
    const next = pairs.slice()
    next[i] = { ...next[i], [key]: v }
    push(next)
  }

  const setRecordIndex = (v: string) => onChange({ ...values, recordIndex: v })

  const serialized = useMemo(() => serialize(pairs), [pairs])

  return (
    <div className="flex flex-col gap-2">
      <label className="flex items-center gap-2">
        <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          record index
        </span>
        <input
          type="number"
          value={values.recordIndex ?? '0'}
          onChange={(e) => setRecordIndex(e.target.value)}
          className="w-20 rounded border px-2 py-1 text-[12px]"
          style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
        />
      </label>

      <div className="mt-2 flex items-center justify-between">
        <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          fields
        </h4>
        <button
          onClick={add}
          className="rounded border px-2 py-0.5 text-[11px]"
          style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--accent)' }}
        >
          + field
        </button>
      </div>

      {pairs.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No fields yet. Click <strong>+ field</strong> to add one.
        </p>
      )}

      <div className="flex flex-col gap-2">
        {pairs.map((p, i) => (
          <div
            key={i}
            className="grid grid-cols-[1fr_1fr_auto] items-end gap-2 rounded border p-2"
            style={{ borderColor: 'var(--border)', background: '#0a0a14' }}
          >
            <label className="flex flex-col">
              <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>field path</span>
              <input
                value={p.path}
                onChange={(e) => patch(i, 'path', e.target.value)}
                placeholder="order.amount"
                className="rounded border px-2 py-1 text-[12px]"
                style={{ background: 'var(--surface)', borderColor: 'var(--border)', color: 'var(--text)' }}
              />
            </label>
            <label className="flex flex-col">
              <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>attribute</span>
              <input
                value={p.attr}
                onChange={(e) => patch(i, 'attr', e.target.value)}
                placeholder={lastSegment(p.path) || 'amount'}
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
