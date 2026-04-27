import { useMemo } from 'react'
import type { WizardProps } from './index'

// SplitText wizard — the raw `delimiter` param accepts escape sequences
// like `\n`, `\n\n`, `\t`. Operators end up guessing the escape syntax;
// quick-pick buttons make the common cases one click. Custom input
// stays available for anything else.

const PRESETS: { label: string; value: string; note: string }[] = [
  { label: 'newline', value: '\\n', note: 'split at every line break' },
  { label: 'blank line', value: '\\n\\n', note: 'split at paragraph breaks (common for logs)' },
  { label: 'tab', value: '\\t', note: 'split at tabs' },
  { label: 'comma', value: ',', note: 'CSV-like flat split' },
  { label: 'semicolon', value: ';', note: 'alternate list separator' },
]

export function SplitTextWizard({ values, onChange }: WizardProps) {
  const delimiter = values.delimiter ?? ''
  const headerLines = values.headerLines ?? '0'

  const activePreset = useMemo(
    () => PRESETS.find((p) => p.value === delimiter)?.value ?? null,
    [delimiter],
  )

  const set = (k: string, v: string) => onChange({ ...values, [k]: v })

  return (
    <div className="flex flex-col gap-3">
      <div>
        <div className="mb-1 flex items-baseline justify-between">
          <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>delimiter</span>
          <span className="text-[10px]" style={{ color: 'var(--text-muted)' }}>escape sequences OK (\n, \t)</span>
        </div>
        <div className="mb-2 flex flex-wrap gap-1.5">
          {PRESETS.map((p) => (
            <button
              key={p.label}
              onClick={() => set('delimiter', p.value)}
              className="rounded border px-2 py-1 text-[11px]"
              style={{
                background: activePreset === p.value ? '#0f3460' : 'var(--surface-2)',
                borderColor: activePreset === p.value ? 'var(--accent)' : 'var(--border)',
                color: 'var(--text)',
              }}
              title={p.note}
              type="button"
            >
              {p.label}
            </button>
          ))}
        </div>
        <input
          value={delimiter}
          onChange={(e) => set('delimiter', e.target.value)}
          placeholder="\\n\\n"
          className="w-full rounded border px-2 py-1 font-mono text-[12px]"
          style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
        />
      </div>

      <label className="flex items-center gap-2">
        <span className="text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>header lines to skip</span>
        <input
          type="number"
          min={0}
          value={headerLines}
          onChange={(e) => set('headerLines', e.target.value)}
          className="w-20 rounded border px-2 py-1 text-[12px]"
          style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
        />
      </label>
    </div>
  )
}
