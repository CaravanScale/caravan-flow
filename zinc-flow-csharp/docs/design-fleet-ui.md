# Design — zinc-flow fleet UI

> **Status (2026-04-13): APPROVED, EXECUTION DEFERRED.**
> Captured for later pickup. Resume from Phase 0a (layered config loading).

## Why this exists

zinc-flow is heading to Kubernetes. Each worker pod runs the engine
and nothing else; bundling a UI inside every worker is wasted weight,
creates the "which pod hosts the UI?" problem, and makes upgrades
awkward. NiFi 1 binds the UI to one node. DeltaFi has no good UI
story. Both are the wrong shape for a fleet.

This document captures the design for a **headless worker + separate
UI binary** model that fits K8s naturally and gives operators a
real diagnostic + authoring surface.

## What we're building

- **Worker** (current `zinc-flow`): engine + management API + metrics.
  No UI. Scales horizontally as a K8s Deployment.
- **UI** (new `zinc-flow-ui`): tiny ASP.NET host serving a Blazor
  WebAssembly SPA. Maintains a registry of known workers (static list
  + optional self-registration). Fans queries out, aggregates results
  into single-pane-of-glass views.

```
+----------------------+         +----------------------+
| zinc-flow-ui (NEW)   |         | zinc-flow (worker 1) |
| Blazor WASM SPA      |--HTTP-->| engine + mgmt API    |
| Node registry        |         | optional: register   |
|   - static list      |<--POST--| heartbeat to UI      |
|   - self-registered  |         +----------------------+
|   - K8s discovery    |
|     (later)          |         +----------------------+
| Aggregated views     |--HTTP-->| zinc-flow (worker N) |
| YAML write-back      |         +----------------------+
| Git integration      |
+----------------------+
```

Two binaries. Workers don't import UI code; UI doesn't import engine
code. Wire protocol is HTTP/JSON only — already the dialect of the
existing management API.

## Killer features (UI's three jobs)

1. **Author flows without YAML** → write back to the targeted worker's
   `config.yaml`, optionally `git commit`/`git push`.
2. **Inspect provenance lineage when things break** → failure-queue
   inspector that works across the whole fleet.
3. **Fix dataflow issues** → edit, save, restart pipeline (hot reload
   nice-to-have).

## Operational requirements

### Layered config files (no placeholder syntax)

`config.yaml` is the base (committed). At boot:
```
config.yaml  ←  config.local.yaml  ←  secrets.yaml
                      (gitignored)         (gitignored)
```
Deep-merge at every dot path. Both overlay files have the **same shape**
as the base config — secrets sit at the same path their committed
counterparts do (`flow.processors.put-http.config.token`). One schema,
three files. Matches the existing nested-config mental model exactly;
introduces no new placeholder syntax.

### Compiled processor versioning

`type: ConvertJSONToRecord@1.2.0` in YAML. Default to latest. Validate
at load. `ProcessorInfo` gains a `Version` field; `Registry._factories`
keyed by both `name` (latest) and `name@version` (pinned).

### No positions in version control

Workflow-style fixed-slot UI (think n8n / Step Functions /
GoHighLevel). The data structure IS the layout — depth from BFS,
branches as columns, "+" to insert. Users can't drag, so there's
nothing to drift. NiFi/DeltaFi positioning churn is structurally
impossible.

## Discovery model

1. **Static list** (always available). UI config: `nodes.static: [...]`.
   In K8s a Service URL or pod IPs.
2. **Worker self-registration** (opt-in per worker). Worker config:
   `ui.register_to: http://zinc-flow-ui.ns.svc:9090`. Worker POSTs
   `{nodeId, url, version, hostname}` on startup, heartbeats every 30s.
   UI's registry is the union of static + self-registered.
3. **K8s service discovery** — deferred. Add a follow-up phase when
   needed.

## Phases (each shippable independently)

### Phase 0 — Worker prep (no UI yet)

Backend capabilities the UI will need, plus operational features that
stand on their own.

- **0a.** Layered config loading (`ConfigLoader.cs`)
- **0b.** Compiled processor versioning (`ProcessorInfo.Version`,
  `name@version` parsing)
- **0c.** YAML emitter (hand-rolled, AOT-clean)
- **0d.** `GET /api/identity` + `UIRegistrationProvider` (worker
  self-registers if `ui.register_to` set)
- **0e.** New worker endpoints: `GET /api/processor-types/{name}`,
  `PUT /api/processors/{name}/config`, `PUT
  /api/processors/{name}/connections`, `POST /api/flow/save`,
  `GET /api/provenance/failures?n=N`,
  `GET /api/provenance/lineage/{flowFileId}`,
  `GET /api/overlays`, `PUT /api/overlays/secrets`
- **0f.** Optional Git provider (worker side, opt-in)
- **0g.** ~40 new test assertions

### Phase 1 — UI binary scaffold (single-worker mode)

New sibling project `zinc-flow-ui-csharp/`. Targets a single worker
first — multi-worker comes in Phase 2.

- Blazor WebAssembly SPA + tiny ASP.NET host
- Pages: `/flow`, `/lineage`, `/settings`, `/nodes`
- Workflow-style flow visualization (no drag, no positions)
- Provenance lineage inspector with failure queue and "highlight in
  graph" cross-link

### Phase 2 — Multi-worker registry + aggregated views

- `NodeRegistry` on UI side (union of static + self-registered)
- Registry endpoints: `POST /api/registry/register`,
  `POST /api/registry/heartbeat`, `GET /api/registry/nodes`
- Aggregated lineage across nodes
- Node selector on `/flow`

### Phase 3 — Editor + persist + git

- "+" to insert processors, click to edit, drag-free wiring
- `Save` (`POST /api/flow/save` to targeted worker)
- `Commit` / `Push` (visible only when worker reports `vc.enabled`)
- Settings → Overlays + Processor Versions

## Stack decisions (locked)

- **UI framework**: Blazor WebAssembly (chosen to avoid NPM/CVE
  exposure, share C# data classes server↔client, stay in the .NET 10
  toolchain we already use)
- **Layout**: workflow-style, no positioning data persisted
- **Secrets**: layered config files (no `${env:VAR}` placeholders)
- **Discovery**: static list + worker self-registration; K8s deferred
- **Git**: opt-in per worker via `vc.enabled: true`; UI never makes
  commits without explicit operator action

## Not in scope (explicit)

- Real-time live throughput per processor in the UI (the existing
  `/api/flow` polling is sufficient; no need for SSE/WebSockets v1)
- Flow editing across multiple workers in one transaction (each
  edit targets one worker)
- K8s API-based discovery (deferred; static + self-registration covers
  the common cases)
- libgit2 — Git ops shell out to the system `git` binary
- Replay-on-different-node feature

## Full plan reference

The detailed implementation plan with file paths, test cases, and
verification steps is at:
`/home/vrjoshi/.claude/plans/kind-orbiting-moth.md`
