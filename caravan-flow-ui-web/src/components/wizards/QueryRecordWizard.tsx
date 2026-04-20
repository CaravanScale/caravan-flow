import { useEffect, useState } from 'react'
import type { WizardProps } from './index'

// QueryRecord.query is a JsonPath string. The full grammar is too
// expression-heavy for a pure click-to-assemble UI, so this wizard
// leans on templates: common shapes as buttons, plus an editor with
// live preview. Operators picking a template get a ready-to-run
// example they can tweak by clicking into the input.

const TEMPLATES: { label: string; query: string; note: string }[] = [
  { label: 'all records', query: '$[*]', note: 'pass every record through' },
  { label: 'field > N', query: '$[?(@.amount > 100)]', note: 'numeric filter' },
  { label: 'field == "x"', query: '$[?(@.tier == "premium")]', note: 'string match' },
  { label: 'field exists', query: '$[?(@.email)]', note: 'field present + truthy' },
  { label: 'field missing', query: '$[?(!@.email)]', note: 'field absent or falsy' },
  { label: 'top-level field', query: '$.amount', note: 'extract single field' },
]

export function QueryRecordWizard({ values, onChange }: WizardProps) {
  const initial = values.query ?? ''
  const [query, setQuery] = useState(initial)

  useEffect(() => setQuery(initial), [initial])

  const push = (q: string) => {
    setQuery(q)
    onChange({ ...values, query: q })
  }

  return (
    <div className="flex flex-col gap-2">
      <h4 className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
        jsonpath query
      </h4>

      <input
        value={query}
        onChange={(e) => push(e.target.value)}
        placeholder="$[?(@.amount > 100)]"
        className="rounded border px-2 py-1 font-mono text-[12px]"
        style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
      />

      <h5 className="mt-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
        templates
      </h5>
      <div className="flex flex-wrap gap-2">
        {TEMPLATES.map((t) => (
          <button
            key={t.label}
            onClick={() => push(t.query)}
            className="rounded border px-2 py-1 text-[11px]"
            style={{ background: 'var(--surface-2)', borderColor: 'var(--border)', color: 'var(--text)' }}
            title={t.note}
          >
            {t.label}
          </button>
        ))}
      </div>

      <p className="mt-2 text-[11px]" style={{ color: 'var(--text-muted)' }}>
        JsonPath (RFC-9535). <code>$</code> is the record root;
        <code> @</code> is the current element inside a filter.
      </p>
    </div>
  )
}
