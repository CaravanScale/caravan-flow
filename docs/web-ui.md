# Web UI — current state and path forward

> **TL;DR.** C# ships a 366-line read-only dashboard (`dashboard.html` + dagre SVG + `/api/flow` polling). Go and Python ship no UI at all. A full authoring fleet UI was designed (Blazor WASM, workflow-style layout, two-binary split) but execution is deferred.
>
> **Immediate win:** embed the existing `dashboard.html` into the Go binary via `//go:embed`. Same SVG, same API, identical look-and-feel across C# and Go in ~40 lines of Zinc.
>
> **Next step:** decide whether fleet UI ships as Blazor (per the existing design, C#-only) or as a Go-native equivalent we can ship with the Go binary natively.

---

## Inventory — what exists today

### zinc-flow-csharp (gold)

- **`ZincFlow/dashboard.html`** — 366-line single-file SPA, committed:
  - Uses dagre from `unpkg.com` (single external script tag) for DAG layout
  - Polls `GET /api/flow` every ~2s
  - Renders SVG: processor nodes with state colour-coding (enabled/disabled/erroring), edge labels for non-`success` relationships, entry/sink badges, live counters, per-source dots
  - **Read-only.** No mutation buttons, no flow editing.
- Served at `/` by `Program.cs` — probes four relative paths (JIT dev, AOT build, CWD variants) and maps the file contents to `GET /`.

### zinc-flow (Go port)

- **No UI.** The management API is complete and mirrors C# (`/api/flow`, `/api/processors`, `/api/dlq`, `/api/providers`, …) but nothing serves HTML on `/`. Hitting `http://localhost:9091/` returns 404.
- The Zinc binary has no static-file-serving infrastructure wired up either — that comes next.

### zinc-flow-python

- **No UI.** Same situation as Go: API present, no dashboard. No current plan to add one (Python port is primarily a DataFrame-integration surface, not a management UI story).

### Fleet UI design

- **`zinc-flow-csharp/docs/design-fleet-ui.md`** — 169-line design doc, **status: APPROVED, EXECUTION DEFERRED.**
- Two-binary model: headless workers + separate `zinc-flow-ui` that fronts a fleet.
- Stack locked: Blazor WebAssembly, workflow-style fixed-slot layout (no drag-to-position), layered config for secrets, opt-in git.
- Four phases documented (0 worker prep, 1 UI scaffold, 2 multi-worker registry, 3 editor + git).

---

## Tier 1 — embed the read-only dashboard into Go (next up)

Quickest path to UI parity with C#. The existing `dashboard.html` already speaks the management-API dialect, which Zinc/Go already serves.

### Work

1. **Copy** `zinc-flow-csharp/ZincFlow/dashboard.html` into zinc-flow root (or into `src/fabric/api/`).
2. **Embed** via Go's `//go:embed` directive — Zinc passes the directive through to the generated Go:
   ```zinc
   //go:embed dashboard.html
   String dashboardHTML
   ```
   (or whatever the current Zinc embed syntax is — if none, add a thin Go shim).
3. **Register root handler** in `main.zn`:
   ```zinc
   HandleFunc("/", api.dashboardHandler)
   ```
4. **Handler:**
   ```zinc
   pub void dashboardHandler(ResponseWriter w, Request r) {
       w.Header().Set("Content-Type", "text/html; charset=utf-8")
       w.Write(dashboardHTML.toBytes())
   }
   ```

### Estimate

- New code: ~40 lines of Zinc.
- Binary-size impact: +15 KB (the HTML is already compressed well by the Go linker).
- Testing: visit `http://localhost:9091/` in a browser, confirm the SVG renders against a running Go binary.

### Caveats

- The SPA depends on `unpkg.com/@dagrejs/dagre@1.1.4` — external CDN fetch. For airgapped deploys, bundle dagre into the embedded HTML (add another ~60 KB).
- Read-only. Authoring still requires editing YAML by hand.

---

## Tier 2 — fleet UI (deferred)

This is the full Blazor WASM effort captured in `zinc-flow-csharp/docs/design-fleet-ui.md`. Repeating the headlines here so we don't lose the thread:

### What it unlocks

1. **Author flows without YAML** — insert/wire/edit processors in the UI, save back to the targeted worker's `config.yaml`, optionally `git commit && git push`.
2. **Fleet-wide lineage inspection** — aggregate provenance queries across all known workers; failure-queue inspector with "highlight in graph" cross-link.
3. **Fix-restart workflow** — edit, save, restart pipeline (hot reload is already shipped in the worker).

### Why it's two binaries

Bundling the UI inside every worker pod wastes resources and creates the "which pod hosts the UI?" problem. NiFi 1 binds the UI to one node; DeltaFi has no good UI story. A separate `zinc-flow-ui` binary sidesteps both.

```
┌──────────────────────┐      ┌──────────────────────┐
│ zinc-flow-ui (NEW)   │      │ zinc-flow (worker 1) │
│ Blazor WASM SPA      │─────▶│ engine + mgmt API    │
│ Node registry        │◀────▶│ optional: register   │
│ Aggregated views     │      └──────────────────────┘
│ YAML write-back      │              ...
│ Optional git         │      ┌──────────────────────┐
└──────────────────────┘─────▶│ zinc-flow (worker N) │
                              └──────────────────────┘
```

### Phases (each shippable independently)

| Phase | Scope |
|---|---|
| **0** | Worker prep — layered config loading, YAML emitter, processor versioning, `/api/identity`, provenance endpoints, optional git provider, ~40 new assertions |
| **1** | UI binary scaffold — single-worker mode, Blazor WASM SPA, `/flow`, `/lineage`, `/settings`, `/nodes` pages |
| **2** | Multi-worker registry — union of static list + self-registered workers, aggregated views |
| **3** | Editor + git — insert/wire/edit processors, save, optional commit/push |

### Full plan

`zinc-flow-csharp/docs/design-fleet-ui.md` has the complete design. Detailed implementation plan (file paths, test cases, verification) was captured at `~/.claude/plans/kind-orbiting-moth.md`.

---

## Tier 3 — Go-native parallel (open question)

The existing fleet-UI design is Blazor-shaped. **Open question:** does the Go port need its own UI binary or does it piggyback on the C# `zinc-flow-ui` once it ships?

### Option A — single UI binary across ports (C#-only fleet UI)

- `zinc-flow-ui-csharp` speaks HTTP/JSON to any worker regardless of runtime.
- Go and Python workers get fleet-UI capability "for free" by exposing the same API.
- **Cost:** operators running an all-Go deployment still need the .NET runtime for the UI pod. That's either an 8-9 MB Blazor WASM app hosted by a ~30 MB AOT .NET server, or a framework-dependent deploy with .NET 10 installed.

### Option B — Go-native fleet UI

Build `zinc-flow-ui-go` alongside the C# one. Two options for the stack:

1. **Go + HTMX + `html/template`** — server renders HTML, HTMX handles partial updates. No JS build step. Tiny binary (~5 MB). Matches the no-NPM constraint already locked in the design.
2. **Go + embedded SPA** — ship a prebuilt Svelte/Solid/React bundle alongside Go templates serving it. More powerful UI but reintroduces NPM/CVE surface the Blazor choice was meant to avoid.

If we go this route, **HTMX is the right match** — it keeps the zero-JS-toolchain discipline, fits Go's stdlib templating, and the authoring UI in the fleet design isn't interaction-heavy (insert processors into fixed slots; no drag-and-drop by design).

### Recommendation

- **Tier 1 now** — embed the existing dashboard, ship UI parity in a day.
- **Tier 2 Option A short-term** — when fleet UI needs become real, build it in Blazor per the existing design. All workers talk HTTP/JSON; the UI doesn't care what compiled them.
- **Tier 2 Option B later if needed** — revisit a Go-native UI only if the .NET-runtime dependency becomes a blocker for all-Go deployments (e.g., airgapped edge where pulling the .NET runtime is a hassle). HTMX is the right Go stack when that time comes.

---

## Decision points (open)

- **Q1: Do we ship Tier 1 this week?** Low-risk, high-visibility. Brings Go to UI parity with C#.
- **Q2: Fleet UI in Blazor (Option A) or Go/HTMX (Option B)?** Blazor is already designed; Go/HTMX is a rewrite. Decide once there's concrete fleet-UI demand.
- **Q3: Bundle dagre into the embedded dashboard vs keep the unpkg CDN?** Bundle +60 KB for airgapped support; CDN keeps binary lean but fails offline.
- **Q4: Authoring UI for single workers too, or fleet-only?** The fleet UI's "save back to `config.yaml`" capability could be useful for a single worker running on a laptop; decide whether single-worker mode exposes editing features from day 1.

---

## Related docs

- [`zinc-flow-csharp/docs/design-fleet-ui.md`](../zinc-flow-csharp/docs/design-fleet-ui.md) — approved fleet-UI design
- [`TODO.md`](../TODO.md) — Phase 2f (Fleet UI) lives there, status: DESIGNED, EXECUTION DEFERRED
- `zinc-flow-csharp/ZincFlow/dashboard.html` — the existing read-only dashboard
