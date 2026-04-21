import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'

// Expression builder modal — Phase D of the wizard push. Surrounds a
// single monospace textarea with field chips, a function palette, and
// an operator toolbar that each insert at the cursor. Live preview
// posts to /api/expression/parse against the processor's most recent
// sample so operators see the evaluated value or the syntax error in
// real time.
//
// No separate "visual" vs "raw" tabs — the textarea IS the expression.
// The difference between casual and power users is just which of the
// surrounding buttons they click. Hand-writing is still possible but
// not necessary to build any supported expression.

interface Props {
  open: boolean
  // Current value of the underlying field when the builder opens.
  initialValue: string
  // Processor name whose sample ring feeds the field picker + preview.
  // Optional: palette-drop builders (new processors mid-add) may not
  // have samples yet; chips + preview degrade gracefully.
  processorName?: string
  onSave: (value: string) => void
  onCancel: () => void
}

const FUNCTIONS = [
  { label: 'upper(|)', snippet: 'upper(', closing: ')', doc: 'Uppercase a string' },
  { label: 'lower(|)', snippet: 'lower(', closing: ')', doc: 'Lowercase a string' },
  { label: 'trim(|)', snippet: 'trim(', closing: ')', doc: 'Strip surrounding whitespace' },
  { label: 'length(|)', snippet: 'length(', closing: ')', doc: 'String length' },
  { label: 'substring(s,i,j)', snippet: 'substring(', closing: ', 0, 1)', doc: 'Substring by indices' },
  { label: 'contains(a,b)', snippet: 'contains(', closing: ', "")', doc: 'True if a contains b' },
  { label: 'startsWith(a,b)', snippet: 'startsWith(', closing: ', "")', doc: 'Prefix test' },
  { label: 'endsWith(a,b)', snippet: 'endsWith(', closing: ', "")', doc: 'Suffix test' },
  { label: 'replace(s,a,b)', snippet: 'replace(', closing: ', "", "")', doc: 'Find/replace' },
  { label: 'concat(...)', snippet: 'concat(', closing: ')', doc: 'Concatenate strings' },
  { label: 'int(x)', snippet: 'int(', closing: ')', doc: 'Cast to integer' },
  { label: 'double(x)', snippet: 'double(', closing: ')', doc: 'Cast to double' },
  { label: 'abs(x)', snippet: 'abs(', closing: ')', doc: 'Absolute value' },
  { label: 'min(a,b)', snippet: 'min(', closing: ', 0)', doc: 'Minimum of two' },
  { label: 'max(a,b)', snippet: 'max(', closing: ', 0)', doc: 'Maximum of two' },
  { label: 'floor(x)', snippet: 'floor(', closing: ')', doc: 'Round down' },
  { label: 'ceil(x)', snippet: 'ceil(', closing: ')', doc: 'Round up' },
  { label: 'round(x)', snippet: 'round(', closing: ')', doc: 'Round to nearest' },
  { label: 'isNull(x)', snippet: 'isNull(', closing: ')', doc: 'True if null' },
  { label: 'isEmpty(x)', snippet: 'isEmpty(', closing: ')', doc: 'True if empty' },
  { label: 'coalesce(...)', snippet: 'coalesce(', closing: ', "")', doc: 'First non-null' },
  { label: 'if(c,a,b)', snippet: 'if(', closing: ', "", "")', doc: 'Ternary' },
]

const OPERATORS = ['+', '-', '*', '/', '%', '==', '!=', '<', '>', '<=', '>=', '&&', '||', '!']

export function ExpressionBuilder({ open, initialValue, processorName, onSave, onCancel }: Props) {
  const [value, setValue] = useState(initialValue)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    if (open) setValue(initialValue)
  }, [open, initialValue])

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onCancel])

  // Samples for field chips + preview context. Query only runs when
  // modal is open and a processor name was passed.
  const samples = useQuery({
    queryKey: ['processor-samples', processorName ?? '__none__'],
    queryFn: () => api.processorSamples(processorName!),
    enabled: open && !!processorName,
    refetchInterval: 2_000,
    staleTime: 2_000,
  })

  const fieldNames = useMemo(() => {
    const s = samples.data?.samples?.[0]
    if (!s) return [] as string[]
    const names = new Set<string>()
    for (const k of Object.keys(s.attributes ?? {})) names.add(k)
    if (s.contentType === 'records' && s.preview) {
      try {
        const p = JSON.parse(s.preview)
        if (p && typeof p === 'object' && !Array.isArray(p))
          for (const k of Object.keys(p)) names.add(k)
      } catch { /* truncated preview — ignore */ }
    }
    return Array.from(names).sort()
  }, [samples.data])

  const contextForPreview = useMemo(() => {
    const s = samples.data?.samples?.[0]
    if (!s) return undefined
    const attributes = { ...(s.attributes ?? {}) }
    let record: Record<string, unknown> | undefined
    if (s.contentType === 'records' && s.preview) {
      try { record = JSON.parse(s.preview) ?? undefined } catch { /* ignore */ }
    }
    return { attributes, record }
  }, [samples.data])

  const [preview, setPreview] = useState<{ ok: boolean; text: string } | null>(null)
  useEffect(() => {
    if (!open) return
    if (value.trim() === '') { setPreview(null); return }
    const t = setTimeout(async () => {
      try {
        const r = await api.expressionParse({ expression: value, context: contextForPreview })
        if (!r.ok) setPreview({ ok: false, text: r.error ?? 'parse error' })
        else if (r.eval === 'error') setPreview({ ok: false, text: 'eval: ' + (r.error ?? '?') })
        else setPreview({ ok: true, text: (r.kind ? `(${r.kind}) ` : '') + (r.value ?? '') })
      } catch (e) {
        setPreview({ ok: false, text: (e as Error).message })
      }
    }, 250)
    return () => clearTimeout(t)
  }, [value, contextForPreview, open])

  const insertAtCursor = (text: string, cursorOffset?: number) => {
    const el = textareaRef.current
    if (!el) {
      setValue((v) => v + text)
      return
    }
    const start = el.selectionStart ?? el.value.length
    const end = el.selectionEnd ?? el.value.length
    const next = el.value.slice(0, start) + text + el.value.slice(end)
    setValue(next)
    const caret = cursorOffset !== undefined ? start + cursorOffset : start + text.length
    // Defer caret restore to after the controlled re-render.
    setTimeout(() => {
      el.focus()
      el.setSelectionRange(caret, caret)
    }, 0)
  }

  const insertFunction = (fn: { snippet: string; closing: string }) => {
    // Insert "fn(" then "<closing>"; caret lands between them so the
    // user types the argument immediately.
    const combined = fn.snippet + fn.closing
    insertAtCursor(combined, fn.snippet.length)
  }

  const insertOperator = (op: string) => {
    insertAtCursor(` ${op} `)
  }

  const insertField = (name: string) => {
    insertAtCursor(name)
  }

  if (!open) return null

  return (
    <>
      <div className="fixed inset-0 z-40" style={{ background: 'rgba(0,0,0,0.55)' }} onClick={onCancel} />
      <div
        role="dialog"
        aria-modal="true"
        className="fixed left-1/2 top-1/2 z-50 flex max-h-[88vh] w-[min(900px,95vw)] -translate-x-1/2 -translate-y-1/2 flex-col rounded-md shadow-2xl"
        style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
      >
        <header
          className="flex items-center justify-between px-4 py-2.5"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)', borderRadius: '6px 6px 0 0' }}
        >
          <strong className="text-[13px]">Expression builder</strong>
          <button onClick={onCancel} className="text-xl leading-none" style={{ color: 'var(--text-muted)' }} aria-label="close">×</button>
        </header>

        <div className="grid flex-1 overflow-hidden" style={{ gridTemplateColumns: '180px 1fr 220px' }}>
          {/* left: field picker */}
          <aside className="flex flex-col overflow-hidden" style={{ borderRight: '1px solid var(--border)' }}>
            <div className="px-3 py-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border)' }}>
              fields
            </div>
            <div className="flex-1 overflow-y-auto p-2">
              {fieldNames.length === 0 && (
                <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
                  {processorName ? 'no samples yet' : 'no processor selected'}
                </p>
              )}
              <div className="flex flex-col gap-1">
                {fieldNames.map((f) => (
                  <button
                    key={f}
                    onClick={() => insertField(f)}
                    className="rounded border px-2 py-1 text-left text-[12px]"
                    style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--accent)' }}
                    title={`insert ${f} at cursor`}
                  >
                    {f}
                  </button>
                ))}
              </div>
            </div>
          </aside>

          {/* center: editor + preview + operator toolbar */}
          <main className="flex flex-col overflow-hidden">
            <div className="flex flex-wrap gap-1 px-2 py-2" style={{ borderBottom: '1px solid var(--border)' }}>
              {OPERATORS.map((op) => (
                <button
                  key={op}
                  onClick={() => insertOperator(op)}
                  className="rounded border px-2 py-1 font-mono text-[11px]"
                  style={{ background: 'var(--surface-2)', borderColor: 'var(--border)', color: 'var(--text)' }}
                >
                  {op}
                </button>
              ))}
            </div>
            <textarea
              ref={textareaRef}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder={'age > 18  ·  tier == "gold"  ·  amount * 0.07'}
              className="flex-1 resize-none p-3 font-mono text-[13px] outline-none"
              style={{ background: '#0a0a14', color: 'var(--text)', minHeight: 140 }}
            />
            <div
              className="px-3 py-2 text-[11px]"
              style={{ borderTop: '1px solid var(--border)', background: '#0a0a14' }}
            >
              {preview === null && <span style={{ color: 'var(--text-muted)' }}>preview appears here as you type</span>}
              {preview && preview.ok && (
                <>
                  <span style={{ color: 'var(--text-muted)' }}>=&gt; </span>
                  <span style={{ color: 'var(--success)' }}>{preview.text}</span>
                </>
              )}
              {preview && !preview.ok && (
                <>
                  <span style={{ color: 'var(--error)' }}>{preview.text}</span>
                </>
              )}
            </div>
          </main>

          {/* right: function palette */}
          <aside className="flex flex-col overflow-hidden" style={{ borderLeft: '1px solid var(--border)' }}>
            <div className="px-3 py-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border)' }}>
              functions
            </div>
            <div className="flex-1 overflow-y-auto p-2">
              <div className="flex flex-col gap-1">
                {FUNCTIONS.map((fn) => (
                  <button
                    key={fn.label}
                    onClick={() => insertFunction(fn)}
                    className="rounded border px-2 py-1 text-left font-mono text-[11px]"
                    style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text)' }}
                    title={fn.doc}
                  >
                    {fn.label}
                  </button>
                ))}
              </div>
            </div>
          </aside>
        </div>

        <footer
          className="flex items-center gap-2 px-4 py-2.5"
          style={{ background: '#10102a', borderTop: '1px solid var(--border)', borderRadius: '0 0 6px 6px' }}
        >
          <button
            onClick={onCancel}
            className="rounded border px-3 py-1 text-[12px]"
            style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
          >
            cancel
          </button>
          <div className="flex-1" />
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            esc to cancel · click fields / operators / functions to assemble
          </span>
          <button
            onClick={() => onSave(value)}
            className="rounded px-4 py-1 text-[12px]"
            style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
          >
            save
          </button>
        </footer>
      </div>
    </>
  )
}
