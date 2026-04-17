import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import type { VcStatus } from '../api/types'

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
          overlay stack: base ← local ← env (secrets no longer loaded from disk)
        </p>
      </header>

      <section className="mb-6">
        <h2 className="mb-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          Overlays
        </h2>
        {overlays.isError && (
          <p className="text-[12px]" style={{ color: 'var(--error)' }}>
            failed to load /api/overlays: {(overlays.error as Error).message}
          </p>
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
          <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
            {(['base', 'local', 'secrets'] as const).map((role) => {
              const layer = findLayer(overlays.data, role)
              return (
                <div
                  key={role}
                  className="rounded border p-3"
                  style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}
                >
                  <div className="mb-2 flex items-center gap-2">
                    <h3 className="text-[12px] font-semibold text-white capitalize">{role}</h3>
                    {role === 'secrets' && (
                      <span
                        className="rounded px-1.5 py-0.5 text-[9px] uppercase tracking-widest"
                        style={{ background: 'var(--surface-2)', color: 'var(--error)', border: '1px solid var(--error)' }}
                      >
                        retired
                      </span>
                    )}
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
                          <dd>{layer.size} B</dd>
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

      <section className="mb-6">
        <h2 className="mb-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          Secrets
        </h2>
        <div
          className="rounded border p-3 text-[12px]"
          style={{ background: '#2a1010', borderColor: 'var(--error)' }}
        >
          <p style={{ color: 'var(--error)' }}>
            Secrets live in environment variables, not on disk.
          </p>
          <p className="mt-1" style={{ color: 'var(--text-muted)' }}>
            The legacy <code>secrets.yaml</code> overlay and its editor were removed. Set
            secret values via env vars on the worker process before launch.
          </p>
        </div>
      </section>

      <section>
        <h2 className="mb-2 text-[10px] uppercase tracking-widest" style={{ color: 'var(--text-muted)' }}>
          Version control
        </h2>
        {vc.isError && (
          <p className="text-[12px]" style={{ color: 'var(--error)' }}>
            failed to load /api/vc/status: {(vc.error as Error).message}
          </p>
        )}
        {vc.isLoading && (
          <p className="text-[12px]" style={{ color: 'var(--text-muted)' }}>loading…</p>
        )}
        {vc.isSuccess && (
          <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-[12px]">
            <dt style={{ color: 'var(--text-muted)' }}>enabled</dt>
            <dd>{String(vc.data.enabled)}</dd>
            {vc.data.enabled && (
              <>
                {vc.data.branch && (
                  <>
                    <dt style={{ color: 'var(--text-muted)' }}>branch</dt>
                    <dd className="font-mono">{vc.data.branch}</dd>
                  </>
                )}
                {vc.data.clean !== undefined && (
                  <>
                    <dt style={{ color: 'var(--text-muted)' }}>clean</dt>
                    <dd>{String(vc.data.clean)}</dd>
                  </>
                )}
                {typeof vc.data.ahead === 'number' && (
                  <>
                    <dt style={{ color: 'var(--text-muted)' }}>ahead</dt>
                    <dd>{vc.data.ahead}</dd>
                  </>
                )}
                {typeof vc.data.behind === 'number' && (
                  <>
                    <dt style={{ color: 'var(--text-muted)' }}>behind</dt>
                    <dd>{vc.data.behind}</dd>
                  </>
                )}
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
