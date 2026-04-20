import { useEffect, useMemo, useState } from 'react'
import type { Flow } from '../api/types'
import { useIngestFlowFile } from '../lib/mutations'

// Test-flowfile injection dialog. Lets the operator hand-craft a FlowFile
// (content + attributes) and push it into the graph — either at a specific
// processor or fanning out to the entry points.
//
// Content is entered as UTF-8 text (covers most practical cases: JSON, CSV,
// plain text). For binary payloads there's a separate base64 field. The
// backend accepts either; content takes priority when both are set.

interface Props {
  open: boolean
  flow: Flow | undefined
  onClose: () => void
}

interface AttrRow { key: string; value: string }

export function TestFlowFileDialog({ open, flow, onClose }: Props) {
  const ingest = useIngestFlowFile()
  const [target, setTarget] = useState('')  // "" = entry points
  const [mode, setMode] = useState<'text' | 'base64'>('text')
  const [content, setContent] = useState('')
  const [attrs, setAttrs] = useState<AttrRow[]>([])
  const [status, setStatus] = useState<string | null>(null)
  const [err, setErr] = useState(false)

  useEffect(() => {
    if (!open) {
      setTarget(''); setMode('text'); setContent(''); setAttrs([]); setStatus(null); setErr(false)
    }
  }, [open])

  const targets = useMemo(() => {
    if (!flow) return []
    return flow.processors.map((p) => p.name).sort()
  }, [flow])

  const submit = async () => {
    setStatus(null); setErr(false)
    const attributes: Record<string, string> = {}
    for (const r of attrs) {
      if (!r.key.trim()) continue
      attributes[r.key] = r.value
    }
    const body: Parameters<typeof ingest.mutateAsync>[0] = { target, attributes }
    if (mode === 'base64') body.contentBase64 = content
    else body.content = content
    try {
      const resp = await ingest.mutateAsync(body)
      const st = resp['status'] as string | undefined
      const ff = resp['flowfile'] as string | undefined
      setStatus(`${st ?? 'ingested'}${ff ? ` · ${ff}` : ''}`)
      if (st !== 'ingested') setErr(true)
    } catch (e) {
      setStatus((e as Error).message); setErr(true)
    }
  }

  if (!open) return null

  return (
    <>
      <div className="fixed inset-0 z-40" style={{ background: 'rgba(0,0,0,0.5)' }} onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        className="fixed left-1/2 top-1/2 z-50 flex max-h-[90vh] w-[min(640px,95vw)] -translate-x-1/2 -translate-y-1/2 flex-col rounded-md shadow-2xl"
        style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
      >
        <header
          className="flex items-center justify-between px-4 py-3"
          style={{ background: 'var(--surface-2)', borderBottom: '1px solid var(--border)', borderRadius: '6px 6px 0 0' }}
        >
          <strong>Send test flowfile</strong>
          <button onClick={onClose} className="text-xl leading-none" style={{ color: 'var(--text-muted)' }} aria-label="close">×</button>
        </header>

        <div className="flex-1 overflow-y-auto p-4 text-[13px]">
          <div className="mb-3">
            <label className="mb-1 block text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              target
            </label>
            <select
              value={target}
              onChange={(e) => setTarget(e.target.value)}
              className="w-full rounded border px-2 py-1"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
            >
              <option value="">{`(entry points — ${flow?.entryPoints.length ?? 0})`}</option>
              {targets.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          <div className="mb-3">
            <label className="mb-1 flex items-center justify-between text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              <span>content</span>
              <span className="flex gap-2 normal-case">
                <label className="flex items-center gap-1">
                  <input type="radio" checked={mode === 'text'} onChange={() => setMode('text')} />
                  <span>utf-8</span>
                </label>
                <label className="flex items-center gap-1">
                  <input type="radio" checked={mode === 'base64'} onChange={() => setMode('base64')} />
                  <span>base64</span>
                </label>
              </span>
            </label>
            <textarea
              value={content}
              onChange={(e) => setContent(e.target.value)}
              placeholder={mode === 'text' ? '{"name":"alice","amount":42}' : 'SGVsbG8gd29ybGQ='}
              rows={6}
              className="w-full rounded border px-2 py-1 font-mono"
              style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
            />
          </div>

          <div className="mb-3">
            <label className="mb-1 block text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
              attributes
            </label>
            <div className="flex flex-col gap-1.5">
              {attrs.map((a, i) => (
                <div key={i} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_24px] items-center gap-2">
                  <input
                    value={a.key}
                    placeholder="key"
                    onChange={(e) => {
                      const next = [...attrs]; next[i] = { ...a, key: e.target.value }; setAttrs(next)
                    }}
                    className="rounded border px-2 py-1"
                    style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                  />
                  <input
                    value={a.value}
                    placeholder="value"
                    onChange={(e) => {
                      const next = [...attrs]; next[i] = { ...a, value: e.target.value }; setAttrs(next)
                    }}
                    className="rounded border px-2 py-1"
                    style={{ background: '#0a0a14', borderColor: 'var(--border)', color: 'var(--text)' }}
                  />
                  <button
                    onClick={() => setAttrs(attrs.filter((_, j) => j !== i))}
                    className="text-lg leading-none"
                    style={{ color: 'var(--text-muted)' }}
                    aria-label="remove"
                    type="button"
                  >×</button>
                </div>
              ))}
            </div>
            <button
              onClick={() => setAttrs([...attrs, { key: '', value: '' }])}
              className="mt-2 rounded border px-2 py-1 text-[11px]"
              style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
              type="button"
            >
              + add attribute
            </button>
          </div>

          {status && (
            <p className="text-[11px]" style={{ color: err ? 'var(--error)' : 'var(--text-muted)' }}>
              {status}
            </p>
          )}
        </div>

        <footer
          className="flex items-center gap-2 px-4 py-3"
          style={{ background: '#10102a', borderTop: '1px solid var(--border)', borderRadius: '0 0 6px 6px' }}
        >
          <button
            onClick={onClose}
            className="rounded border px-3 py-1"
            style={{ background: 'transparent', borderColor: 'var(--border)', color: 'var(--text-muted)' }}
          >
            close
          </button>
          <div className="flex-1" />
          <button
            onClick={submit}
            disabled={ingest.isPending}
            className="rounded px-4 py-1"
            style={{ background: '#0f3460', border: '1px solid var(--accent)', color: '#fff' }}
          >
            {ingest.isPending ? 'sending…' : 'send'}
          </button>
        </footer>
      </div>
    </>
  )
}
