import { useEffect, useMemo, useState } from 'react'
import type { WizardProps } from './index'

// Shared editor for the `fields` parameter on ConvertAvroToRecord and
// ConvertCSVToRecord: a comma-separated list of `name:type` pairs.
// Both processors share the same format; the wizard presents it as a
// row editor so operators don't hand-type "id:long,name:string,amount:double".

const TYPES = ['string', 'int', 'long', 'float', 'double', 'boolean', 'bytes'] as const
type FieldType = typeof TYPES[number]

interface Field {
  name: string
  type: FieldType
}

function parse(raw: string): Field[] {
  const out: Field[] = []
  if (!raw) return out
  for (const chunk of raw.split(',')) {
    const s = chunk.trim()
    if (!s) continue
    const colon = s.indexOf(':')
    const name = colon < 0 ? s : s.slice(0, colon).trim()
    const typeRaw = colon < 0 ? 'string' : s.slice(colon + 1).trim()
    const type = (TYPES as readonly string[]).includes(typeRaw)
      ? (typeRaw as FieldType)
      : 'string'
    out.push({ name, type })
  }
  return out
}

function serialize(fields: Field[]): string {
  return fields
    .filter((f) => f.name.trim().length > 0)
    .map((f) => `${f.name}:${f.type}`)
    .join(',')
}

export function RecordFieldsWizard({ values, onChange }: WizardProps) {
  const initial = values.fields ?? ''
  const [fields, setFields] = useState<Field[]>(() => parse(initial))

  useEffect(() => {
    setFields(parse(initial))
  }, [initial])

  const push = (next: Field[]) => {
    setFields(next)
    onChange({ ...values, fields: serialize(next) })
  }
  const add = () => push([...fields, { name: '', type: 'string' }])
  const remove = (i: number) => push(fields.filter((_, j) => j !== i))
  const patch = (i: number, key: keyof Field, v: string) => {
    const next = fields.slice()
    next[i] = { ...next[i], [key]: v as never }
    push(next)
  }

  const serialized = useMemo(() => serialize(fields), [fields])

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
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

      {fields.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          No fields declared — leave empty to infer from the data, or click <strong>+ field</strong> to pin the schema.
        </p>
      )}

      <div className="flex flex-col gap-1.5">
        {fields.map((f, i) => (
          <div key={i} className="grid grid-cols-[1fr_130px_auto] items-center gap-2">
            <input
              value={f.name}
              onChange={(e) => patch(i, 'name', e.target.value)}
              placeholder="name"
              className="rounded border px-2 py-1 text-[12px]"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
            />
            <select
              value={f.type}
              onChange={(e) => patch(i, 'type', e.target.value)}
              className="rounded border px-2 py-1 text-[12px]"
              style={{ background: 'var(--surface-2)', borderColor: 'var(--border)', color: 'var(--text)' }}
            >
              {TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
            <button
              onClick={() => remove(i)}
              className="px-2 py-1 text-[14px]"
              style={{ color: 'var(--error)' }}
              title="remove"
              type="button"
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
          style={{ background: '#0a0a14', border: '1px solid var(--border)', color: 'var(--text)' }}
        >
          {serialized || '(empty)'}
        </pre>
      </details>
    </div>
  )
}
