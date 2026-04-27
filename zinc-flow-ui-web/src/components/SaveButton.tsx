import { useQuery } from '@tanstack/react-query'
import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../api/client'
import { useSaveFlow } from '../lib/mutations'

// Top-bar save-state widget. Two distinct actions live here:
//
//   1. Auto-save to disk — fires continuously (debounced) whenever the
//      runtime graph drifts from the last-written config.yaml. No git.
//      This is how edge/IoT operators get their edits persisted without
//      thinking about it.
//   2. Commit & push — explicit dev action, triggers git commit + push
//      via the VersionControlProvider. Only visible when VC is enabled.
//
// Poll /api/flow/status every 2 s to track dirty state; the badge shows
// "saving…", "saved", or "disk only" depending on what's true right now.

const STATUS_POLL_MS = 2_000
const AUTO_SAVE_DEBOUNCE_MS = 1_500

export function SaveButton() {
  const vc = useQuery({
    queryKey: ['vc-status'],
    queryFn: api.vcStatus,
    refetchInterval: 30_000,
    staleTime: 30_000,
  })
  const flowStatus = useQuery({
    queryKey: ['flow-status'],
    queryFn: api.flowStatus,
    refetchInterval: STATUS_POLL_MS,
    staleTime: 0,
  })
  const save = useSaveFlow()
  const [commitMsg, setCommitMsg] = useState<string | null>(null)
  const [commitErr, setCommitErr] = useState(false)

  // Debounced auto-save: when the runtime becomes dirty, schedule a
  // disk-only write 1.5 s later. Additional mutations during that
  // window push the timer out, so a flurry of rapid edits collapses
  // into a single write.
  const autoSaveTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const autoSaving = useRef(false)

  const dirty = flowStatus.data?.dirty ?? false

  const triggerAutoSave = useCallback(async () => {
    if (autoSaving.current) return
    autoSaving.current = true
    try {
      await save.mutateAsync({ commit: false })
    } catch {
      /* the polled status will still show dirty; operator can commit
         manually to surface the error. */
    } finally {
      autoSaving.current = false
    }
  }, [save])

  useEffect(() => {
    if (!dirty) return
    if (autoSaveTimer.current) clearTimeout(autoSaveTimer.current)
    autoSaveTimer.current = setTimeout(() => {
      triggerAutoSave()
    }, AUTO_SAVE_DEBOUNCE_MS)
    return () => {
      if (autoSaveTimer.current) { clearTimeout(autoSaveTimer.current); autoSaveTimer.current = null }
    }
  }, [dirty, triggerAutoSave])

  useEffect(() => {
    if (!commitMsg) return
    const t = setTimeout(() => setCommitMsg(null), 6000)
    return () => clearTimeout(t)
  }, [commitMsg])

  const vcEnabled = vc.data?.enabled ?? false
  const vcClean = vc.data?.clean ?? false
  const ahead = vc.data?.ahead ?? 0
  const behind = vc.data?.behind ?? 0
  const branch = vc.data?.branch ?? ''

  const onCommit = async () => {
    setCommitMsg(null); setCommitErr(false)
    try {
      const body = await save.mutateAsync({ message: 'flow: update via UI', push: true, commit: true })
      const parts: string[] = []
      if (typeof body.bytes === 'number') parts.push(`saved ${body.bytes}B`)
      else parts.push('saved')
      if (body.committed) parts.push('committed')
      if (body.pushed) parts.push('pushed')
      setCommitMsg(parts.join(' · '))
    } catch (e) {
      setCommitMsg(`error: ${(e as Error).message}`)
      setCommitErr(true)
    }
  }

  const pillBase = 'rounded-full border px-2.5 py-0.5 text-[11px] inline-flex items-center gap-1'

  // Save-state badge (auto-save, not git).
  let saveBadge: React.ReactElement
  if (save.isPending || autoSaving.current) {
    saveBadge = (
      <span className={pillBase} style={{ color: 'var(--text-muted)', borderColor: 'var(--border)' }}>
        saving…
      </span>
    )
  } else if (dirty) {
    saveBadge = (
      <span className={pillBase} title="runtime graph has unsaved edits — auto-save pending"
            style={{ color: 'var(--warning)', borderColor: '#3a2f10' }}>
        unsaved
      </span>
    )
  } else {
    saveBadge = (
      <span className={pillBase} title="runtime graph is in sync with config.yaml"
            style={{ color: 'var(--success)', borderColor: '#0f3460' }}>
        saved ✓
      </span>
    )
  }

  return (
    <div className="flex items-center gap-2">
      {saveBadge}

      {vcEnabled && (
        <span
          className={pillBase}
          title={`branch: ${branch}`}
          style={{
            color: vcClean && ahead === 0 && behind === 0 ? 'var(--success)' : 'var(--warning)',
            borderColor: vcClean && ahead === 0 && behind === 0 ? '#0f3460' : '#3a2f10',
          }}
        >
          {branch || 'vc'}
          {ahead > 0 && ` ↑${ahead}`}
          {behind > 0 && ` ↓${behind}`}
          {vcClean && ahead === 0 && behind === 0 && ' ✓'}
        </span>
      )}

      {vcEnabled && (
        <button
          onClick={onCommit}
          disabled={save.isPending}
          className="rounded px-3 py-1 text-[12px]"
          style={{
            background: '#0f3460',
            border: '1px solid var(--accent)',
            color: '#fff',
            opacity: save.isPending ? 0.6 : 1,
          }}
          title="commit + push config.yaml to git"
        >
          {save.isPending ? 'committing…' : 'commit & push'}
        </button>
      )}

      {commitMsg && (
        <span className="text-[11px]" style={{ color: commitErr ? 'var(--error)' : 'var(--text-muted)' }}>
          {commitMsg}
        </span>
      )}
    </div>
  )
}
