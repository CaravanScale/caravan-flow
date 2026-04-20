import { useMemo } from 'react'
import type { ParamInfo, ParamKind } from '../api/types'

// Schema-driven processor config form. Renders an input per ParamInfo
// based on its Kind. Keeps all state as strings in the parent
// (values: Record<string,string>) because the backend config format is
// a flat string->string map. Structured widgets (KeyValueList,
// StringList) parse/serialize their string representation locally.
//
// Default semantics:
//   default = null  -> "no default, leave blank"
//   default = ""    -> "default is an empty string"
// AddProcessorDialog seeds values with (default ?? '') when picking a type.

interface Props {
  parameters: ParamInfo[]
  values: Record<string, string>
  onChange: (next: Record<string, string>) => void
}

const INPUT_CLASS = 'w-full rounded border px-2 py-1'
const INPUT_STYLE = { background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' } as const

export function ProcessorConfigForm({ parameters, values, onChange }: Props) {
  if (parameters.length === 0) {
    return (
      <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
        no configuration
      </p>
    )
  }

  const set = (name: string, value: string) => onChange({ ...values, [name]: value })

  return (
    <div className="flex flex-col gap-3">
      {parameters.map((p) => (
        <ParamField key={p.name} param={p} value={values[p.name] ?? ''} onChange={(v) => set(p.name, v)} />
      ))}
    </div>
  )
}

interface FieldProps {
  param: ParamInfo
  value: string
  onChange: (v: string) => void
}

function ParamField({ param, value, onChange }: FieldProps) {
  return (
    <div>
      <label className="mb-1 flex items-baseline gap-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
        <span>{param.label || param.name}</span>
        {param.required && <span style={{ color: 'var(--error)' }}>required</span>}
      </label>
      <FieldInput param={param} value={value} onChange={onChange} />
      {param.description && (
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          {param.description}
        </p>
      )}
    </div>
  )
}

function FieldInput({ param, value, onChange }: FieldProps) {
  switch (param.kind) {
    case 'Multiline':
    case 'Expression':
      return (
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={param.placeholder ?? ''}
          rows={3}
          className={`${INPUT_CLASS} ${param.kind === 'Expression' ? 'font-mono' : ''}`}
          style={INPUT_STYLE}
        />
      )
    case 'Integer':
    case 'Number':
      return (
        <input
          type="number"
          step={param.kind === 'Integer' ? 1 : 'any'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={param.placeholder ?? ''}
          className={INPUT_CLASS}
          style={INPUT_STYLE}
        />
      )
    case 'Boolean':
      return (
        <label className="flex items-center gap-2 text-[13px]" style={{ color: 'var(--text)' }}>
          <input
            type="checkbox"
            checked={value === 'true'}
            onChange={(e) => onChange(e.target.checked ? 'true' : 'false')}
          />
          <span>{value === 'true' ? 'true' : 'false'}</span>
        </label>
      )
    case 'Enum':
      return (
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className={INPUT_CLASS}
          style={INPUT_STYLE}
        >
          {(param.choices ?? []).map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      )
    case 'Secret':
      return (
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={param.placeholder ?? '${MY_SECRET}'}
          className={`${INPUT_CLASS} font-mono`}
          style={INPUT_STYLE}
        />
      )
    case 'StringList':
      return <StringListInput param={param} value={value} onChange={onChange} />
    case 'KeyValueList':
      return <KeyValueListInput param={param} value={value} onChange={onChange} />
    case 'String':
    default:
      return (
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={param.placeholder ?? ''}
          className={INPUT_CLASS}
          style={INPUT_STYLE}
        />
      )
  }
}

// --- StringList ---

function StringListInput({ param, value, onChange }: FieldProps) {
  const entries = useMemo(() => parseStringList(value, param.entryDelim), [value, param.entryDelim])

  const update = (next: string[]) => onChange(next.join(param.entryDelim))

  return (
    <div className="flex flex-col gap-1.5">
      {entries.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          no entries — add one below
        </p>
      )}
      {entries.map((s, i) => (
        <div key={i} className="grid grid-cols-[minmax(0,1fr)_24px] items-center gap-2">
          <input
            type="text"
            value={s}
            placeholder={param.placeholder ?? ''}
            onChange={(e) => {
              const next = [...entries]
              next[i] = e.target.value
              update(next)
            }}
            className="rounded border px-2 py-1"
            style={INPUT_STYLE}
          />
          <button
            onClick={() => update(entries.filter((_, j) => j !== i))}
            className="text-lg leading-none"
            style={{ color: 'var(--text-muted)' }}
            aria-label="remove"
            type="button"
          >
            ×
          </button>
        </div>
      ))}
      <button
        onClick={() => update([...entries, ''])}
        className="mt-1 self-start rounded border px-2 py-1 text-[11px]"
        style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
        type="button"
      >
        + add
      </button>
    </div>
  )
}

// --- KeyValueList ---

interface Row {
  key: string
  value: string
}

function KeyValueListInput({ param, value, onChange }: FieldProps) {
  const rows = useMemo(() => parseKeyValueList(value, param.entryDelim, param.pairDelim), [value, param.entryDelim, param.pairDelim])
  const valueKind: ParamKind = param.valueKind ?? 'String'

  const update = (next: Row[]) =>
    onChange(next.map((r) => `${r.key}${param.pairDelim}${r.value}`).join(param.entryDelim))

  const setRow = (i: number, patch: Partial<Row>) => {
    const next = rows.map((r, j) => (j === i ? { ...r, ...patch } : r))
    update(next)
  }

  return (
    <div className="flex flex-col gap-1.5">
      {rows.length === 0 && (
        <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
          no entries — add one below
        </p>
      )}
      {rows.map((row, i) => (
        <div
          key={i}
          className="grid items-start gap-2"
          style={{ gridTemplateColumns: 'minmax(0,1fr) minmax(0,1.6fr) 24px' }}
        >
          <input
            type="text"
            value={row.key}
            placeholder="name"
            onChange={(e) => setRow(i, { key: e.target.value })}
            className="rounded border px-2 py-1"
            style={INPUT_STYLE}
          />
          {valueKind === 'Expression' || valueKind === 'Multiline' ? (
            <textarea
              value={row.value}
              placeholder="expression"
              onChange={(e) => setRow(i, { value: e.target.value })}
              rows={1}
              className="rounded border px-2 py-1 font-mono"
              style={INPUT_STYLE}
            />
          ) : (
            <input
              type={valueKind === 'Integer' || valueKind === 'Number' ? 'number' : 'text'}
              value={row.value}
              placeholder="value"
              onChange={(e) => setRow(i, { value: e.target.value })}
              className="rounded border px-2 py-1"
              style={INPUT_STYLE}
            />
          )}
          <button
            onClick={() => update(rows.filter((_, j) => j !== i))}
            className="self-center text-lg leading-none"
            style={{ color: 'var(--text-muted)' }}
            aria-label="remove"
            type="button"
          >
            ×
          </button>
        </div>
      ))}
      <button
        onClick={() => update([...rows, { key: '', value: '' }])}
        className="mt-1 self-start rounded border px-2 py-1 text-[11px]"
        style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
        type="button"
      >
        + add row
      </button>
      {param.placeholder && rows.length === 0 && (
        <p className="text-[11px] font-mono" style={{ color: 'var(--text-muted)' }}>
          example: {param.placeholder}
        </p>
      )}
    </div>
  )
}

// --- Parsing helpers (string <-> widget rows) ---

function parseStringList(raw: string, entryDelim: string): string[] {
  if (!raw) return []
  return raw
    .split(entryDelim)
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}

function parseKeyValueList(raw: string, entryDelim: string, pairDelim: string): Row[] {
  if (!raw) return []
  const out: Row[] = []
  for (const entry of raw.split(entryDelim)) {
    const trimmed = entry.trim()
    if (!trimmed) continue
    const idx = trimmed.indexOf(pairDelim)
    if (idx < 0) {
      out.push({ key: trimmed, value: '' })
    } else {
      out.push({ key: trimmed.slice(0, idx).trim(), value: trimmed.slice(idx + pairDelim.length).trim() })
    }
  }
  return out
}
