import { useEffect, useRef, useState } from 'react'
import type { ParamInfo, ParamKind } from '../api/types'
import { ExpressionBuilder } from './ExpressionBuilder'
import { wizardRegistry } from './wizards'

// Listen for Peek field-pick events (fired by SamplePanel chip clicks).
// When the operator has focus in a text input / textarea, insert the
// field path at the cursor. The event is global; any mounted form
// listens, and whichever input owns the focus wins. Avoids prop-
// drilling a callback through every nested FieldInput.
function useFieldPickListener() {
  useEffect(() => {
    const handler = (ev: Event) => {
      const ce = ev as CustomEvent<{ path: string }>
      const path = ce.detail?.path
      if (!path) return
      const el = document.activeElement as HTMLInputElement | HTMLTextAreaElement | null
      if (!el || (el.tagName !== 'INPUT' && el.tagName !== 'TEXTAREA')) return
      const start = el.selectionStart ?? el.value.length
      const end = el.selectionEnd ?? el.value.length
      const next = el.value.slice(0, start) + path + el.value.slice(end)
      // Setting .value bypasses React state; dispatch a synthetic input
      // event so the controlled component sees the change.
      const setter = Object.getOwnPropertyDescriptor(
        el.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype,
        'value',
      )?.set
      setter?.call(el, next)
      el.dispatchEvent(new Event('input', { bubbles: true }))
      const caret = start + path.length
      el.setSelectionRange(caret, caret)
      el.focus()
    }
    window.addEventListener('zinc:field-pick', handler)
    return () => window.removeEventListener('zinc:field-pick', handler)
  }, [])
}

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
  // Source of samples for the Expression builder's field picker + live
  // preview. Optional because add-processor-dialog forms don't have a
  // running processor yet; the builder degrades to an editor + palette
  // without sample-aware preview.
  processorName?: string
  // Opt-in processor-level wizard id (from RegistryEntry.wizardComponent).
  // When set and known in wizardRegistry, the wizard replaces the
  // generic per-kind form. An "advanced" toggle still exposes the
  // generic fields as an escape hatch.
  wizardComponent?: string | null
}

const INPUT_CLASS = 'w-full rounded border px-2 py-1'
const INPUT_STYLE = { background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' } as const

export function ProcessorConfigForm({ parameters, values, onChange, processorName, wizardComponent }: Props) {
  useFieldPickListener()
  // Expression builder modal state. Only one builder open at a time;
  // a save-callback routes the assembled expression back to whichever
  // input opened it (tracked by a path like "paramName" or
  // "paramName:rowIdx"). Simpler than per-input modal state and keeps
  // the component tree flat.
  const [builderTarget, setBuilderTarget] = useState<{
    initial: string
    save: (v: string) => void
  } | null>(null)

  // When a wizard is registered for this processor, render it in place
  // of the generic form. Operators can still reach the raw parameters
  // via the "advanced" disclosure so nothing is permanently hidden.
  const [showAdvanced, setShowAdvanced] = useState(false)
  const Wizard = wizardComponent ? wizardRegistry[wizardComponent] : undefined

  if (parameters.length === 0) {
    return (
      <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
        no configuration
      </p>
    )
  }

  const set = (name: string, value: string) => onChange({ ...values, [name]: value })

  if (Wizard) {
    return (
      <div className="flex flex-col gap-3">
        <Wizard
          processorName={processorName ?? ''}
          values={values}
          onChange={onChange}
        />
        <button
          onClick={() => setShowAdvanced((v) => !v)}
          className="self-start text-[10px] uppercase tracking-widest"
          style={{ color: 'var(--text-muted)' }}
        >
          {showAdvanced ? '▾ advanced (raw)' : '▸ advanced (raw)'}
        </button>
        {showAdvanced && (
          <div className="rounded border p-2" style={{ borderColor: 'var(--border)', background: '#0a0a14' }}>
            {parameters.map((p) => (
              <ParamField
                key={p.name}
                param={p}
                value={values[p.name] ?? ''}
                onChange={(v) => set(p.name, v)}
                onOpenBuilder={setBuilderTarget}
              />
            ))}
          </div>
        )}
        {builderTarget && (
          <ExpressionBuilder
            open
            initialValue={builderTarget.initial}
            processorName={processorName}
            onSave={(v) => {
              builderTarget.save(v)
              setBuilderTarget(null)
            }}
            onCancel={() => setBuilderTarget(null)}
          />
        )}
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-3">
      {parameters.map((p) => (
        <ParamField
          key={p.name}
          param={p}
          value={values[p.name] ?? ''}
          onChange={(v) => set(p.name, v)}
          onOpenBuilder={setBuilderTarget}
        />
      ))}
      {builderTarget && (
        <ExpressionBuilder
          open
          initialValue={builderTarget.initial}
          processorName={processorName}
          onSave={(v) => {
            builderTarget.save(v)
            setBuilderTarget(null)
          }}
          onCancel={() => setBuilderTarget(null)}
        />
      )}
    </div>
  )
}

interface FieldProps {
  param: ParamInfo
  value: string
  onChange: (v: string) => void
  onOpenBuilder?: (target: { initial: string; save: (v: string) => void }) => void
}

function ParamField({ param, value, onChange, onOpenBuilder }: FieldProps) {
  return (
    <div>
      <label className="mb-1 flex items-baseline justify-between gap-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
        <span className="flex items-baseline gap-2">
          <span>{param.label || param.name}</span>
          {param.required && <span style={{ color: 'var(--error)' }}>required</span>}
        </span>
        {param.kind === 'Expression' && onOpenBuilder && (
          <button
            onClick={() => onOpenBuilder({ initial: value, save: onChange })}
            className="rounded border px-1.5 py-0.5 text-[9px]"
            style={{ background: 'transparent', borderColor: 'var(--accent)', color: 'var(--accent)' }}
            title="open expression builder"
            type="button"
          >
            ✎ builder
          </button>
        )}
      </label>
      <FieldInput param={param} value={value} onChange={onChange} onOpenBuilder={onOpenBuilder} />
      {param.description && (
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          {param.description}
        </p>
      )}
    </div>
  )
}

function FieldInput({ param, value, onChange, onOpenBuilder }: FieldProps) {
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
          <span>{value === 'true' ? 'yes' : 'no'}</span>
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
      return <KeyValueListInput param={param} value={value} onChange={onChange} onOpenBuilder={onOpenBuilder} />
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
  // Local state as source of truth: a single empty entry serializes to ""
  // (`[""].join(";")` = `""`), which would round-trip back to an empty
  // list via parseStringList's early return and swallow the "+ add".
  // Keeping entries locally lets the UI show empty rows while the
  // parent holds the serialized string.
  const [entries, setEntries] = useState<string[]>(() => parseStringList(value, param.entryDelim))
  const lastExternal = useRef(value)
  useEffect(() => {
    if (value !== lastExternal.current) {
      lastExternal.current = value
      setEntries(parseStringList(value, param.entryDelim))
    }
  }, [value, param.entryDelim])

  const update = (next: string[]) => {
    setEntries(next)
    const serialized = next.join(param.entryDelim)
    lastExternal.current = serialized
    onChange(serialized)
  }

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

function KeyValueListInput({ param, value, onChange, onOpenBuilder }: FieldProps) {
  // Local state authoritative — same reason as StringListInput: an
  // empty row serializes to "=" which parse still drops, so round-
  // tripping via the string eats "+ add". Keep rows locally; serialize
  // on mutation; re-sync only on genuine external change.
  const [rows, setRows] = useState<Row[]>(() => parseKeyValueList(value, param.entryDelim, param.pairDelim))
  const lastExternal = useRef(value)
  useEffect(() => {
    if (value !== lastExternal.current) {
      lastExternal.current = value
      setRows(parseKeyValueList(value, param.entryDelim, param.pairDelim))
    }
  }, [value, param.entryDelim, param.pairDelim])

  const valueKind: ParamKind = param.valueKind ?? 'String'
  const expressionValue = valueKind === 'Expression'

  const update = (next: Row[]) => {
    setRows(next)
    const serialized = next.map((r) => `${r.key}${param.pairDelim}${r.value}`).join(param.entryDelim)
    lastExternal.current = serialized
    onChange(serialized)
  }

  const setRow = (i: number, patch: Partial<Row>) => {
    const next = rows.map((r, j) => (j === i ? { ...r, ...patch } : r))
    update(next)
  }

  // For rows with Expression values, add a third column with a "✎"
  // builder-open button. Keeps the raw textarea editable too — power
  // users can still type; anyone else hits the wizard.
  const rowCols = expressionValue
    ? 'minmax(0,1fr) minmax(0,1.6fr) 24px 24px'
    : 'minmax(0,1fr) minmax(0,1.6fr) 24px'

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
          style={{ gridTemplateColumns: rowCols }}
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
          {expressionValue && onOpenBuilder && (
            <button
              onClick={() => onOpenBuilder({
                initial: row.value,
                save: (v: string) => setRow(i, { value: v }),
              })}
              className="self-center text-[12px]"
              style={{ color: 'var(--accent)' }}
              title="open expression builder"
              type="button"
            >
              ✎
            </button>
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
  // Keep empty entries — an empty row is a valid in-progress edit
  // (operator just clicked "+ add" and hasn't typed yet). The backend
  // parser trims empties on save so there's no round-trip surprise.
  return raw.split(entryDelim).map((s) => s.trim())
}

function parseKeyValueList(raw: string, entryDelim: string, pairDelim: string): Row[] {
  if (!raw) return []
  const out: Row[] = []
  for (const entry of raw.split(entryDelim)) {
    const trimmed = entry.trim()
    // Keep empty rows too — "+ add" needs to land a visible row
    // even before the operator types a key. Backend trims on save.
    const idx = trimmed.indexOf(pairDelim)
    if (idx < 0) {
      out.push({ key: trimmed, value: '' })
    } else {
      out.push({ key: trimmed.slice(0, idx).trim(), value: trimmed.slice(idx + pairDelim.length).trim() })
    }
  }
  return out
}
