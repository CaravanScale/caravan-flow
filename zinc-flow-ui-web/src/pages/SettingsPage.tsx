import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { VcStatus } from '../api/types'

function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return '—'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

interface OverlayLayer {
  role?: string
  path?: string
  present?: boolean
  size?: number
}

interface OverlaysShape {
  layers?: OverlayLayer[]
  error?: string
}

function findLayer(data: OverlaysShape | undefined, role: string): OverlayLayer | null {
  if (!data?.layers) return null
  return data.layers.find((l) => l.role === role) ?? null
}

export function SettingsPage() {
  const overlays = useQuery({
    queryKey: ['overlays'],
    queryFn: () => api.overlays() as Promise<OverlaysShape>,
  })
  const vc = useQuery({
    queryKey: ['vc-status'],
    queryFn: () => api.vcStatus() as Promise<VcStatus>,
  })

  return (
    <div className="flex h-full flex-col overflow-auto p-6">
      <header className="mb-4">
        <h1 className="text-base font-semibold text-white">Settings</h1>
        <p className="mt-1 text-[11px]" style={{ color: 'var(--text-muted)' }}>
          overlay stack: base ← local ← env
        </p>
      </header>

      <section className="mb-6">
        <h2 className="mb-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          Overlays
        </h2>
        {overlays.isError && (
          <div className="flex items-center gap-2 text-[12px]" style={{ color: 'var(--error)' }}>
            <span>failed to load /api/overlays: {(overlays.error as Error).message}</span>
            <button
              onClick={() => overlays.refetch()}
              className="rounded border px-2 py-0.5 text-[11px]"
              style={{ background: 'transparent', borderColor: 'var(--error)', color: 'var(--error)' }}
            >
              retry
            </button>
          </div>
        )}
        {overlays.isLoading && (
          <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
        )}
        {overlays.isSuccess && overlays.data?.error && (
          <p className="text-[12px]" style={{ color: 'var(--error)' }}>
            overlays: {overlays.data.error}
          </p>
        )}
        {overlays.isSuccess && !overlays.data?.error && (
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            {(['base', 'local'] as const).map((role) => {
              const layer = findLayer(overlays.data, role)
              return (
                <div
                  key={role}
                  className="rounded border p-3"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}
                >
                  <div className="mb-2 flex items-center gap-2">
                    <h3 className="text-[12px] font-semibold text-white capitalize">{role}</h3>
                  </div>
                  {!layer && (
                    <p className="text-[11px]" style={{ color: 'var(--text-muted)' }}>not reported</p>
                  )}
                  {layer && (
                    <dl className="grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 text-[11px]">
                      {layer.present !== undefined && (
                        <>
                          <dt style={{ color: 'var(--text-muted)' }}>present</dt>
                          <dd>{String(layer.present)}</dd>
                        </>
                      )}
                      {layer.path && (
                        <>
                          <dt style={{ color: 'var(--text-muted)' }}>path</dt>
                          <dd className="truncate font-mono" title={layer.path}>{layer.path}</dd>
                        </>
                      )}
                      {typeof layer.size === 'number' && (
                        <>
                          <dt style={{ color: 'var(--text-muted)' }}>size</dt>
                          <dd>{formatBytes(layer.size)}</dd>
                        </>
                      )}
                    </dl>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </section>

      <section>
        <h2 className="mb-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          Version control
        </h2>
        {vc.isError && (
          <div className="flex items-center gap-2 text-[12px]" style={{ color: 'var(--error)' }}>
            <span>failed to load /api/vc/status: {(vc.error as Error).message}</span>
            <button
              onClick={() => vc.refetch()}
              className="rounded border px-2 py-0.5 text-[11px]"
              style={{ background: 'transparent', borderColor: 'var(--error)', color: 'var(--error)' }}
            >
              retry
            </button>
          </div>
        )}
        {vc.isLoading && (
          <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
        )}
        {vc.isSuccess && !vc.data.enabled && (
          <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>
            Version control is disabled. Set <code>vc.enabled: true</code> in <code>config.yaml</code> to track flow changes in git.
          </p>
        )}
        {vc.isSuccess && vc.data.enabled && (
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-[12px]">
            {vc.data.branch && (
              <>
                <dt style={{ color: 'var(--text-muted)' }}>branch</dt>
                <dd className="font-mono">{vc.data.branch}</dd>
              </>
            )}
            {vc.data.clean !== undefined && (
              <>
                <dt style={{ color: 'var(--text-muted)' }}>working tree</dt>
                <dd style={{ color: vc.data.clean ? 'var(--success)' : 'var(--warning)' }}>
                  {vc.data.clean ? '✓ clean' : '● uncommitted changes'}
                </dd>
              </>
            )}
            {typeof vc.data.ahead === 'number' && (
              <>
                <dt style={{ color: 'var(--text-muted)' }} title="commits made locally that haven't been pushed to the remote">
                  ahead of remote
                </dt>
                <dd>{vc.data.ahead === 0 ? 'in sync' : `${vc.data.ahead} commit${vc.data.ahead === 1 ? '' : 's'}`}</dd>
              </>
            )}
            {typeof vc.data.behind === 'number' && (
              <>
                <dt style={{ color: 'var(--text-muted)' }} title="commits on the remote that haven't been pulled locally">
                  behind remote
                </dt>
                <dd>{vc.data.behind === 0 ? 'in sync' : `${vc.data.behind} commit${vc.data.behind === 1 ? '' : 's'}`}</dd>
              </>
            )}
            {vc.data.error && (
              <>
                <dt style={{ color: 'var(--text-muted)' }}>error</dt>
                <dd style={{ color: 'var(--error)' }}>{vc.data.error}</dd>
              </>
            )}
          </dl>
        )}
      </section>
    </div>
  )
}
