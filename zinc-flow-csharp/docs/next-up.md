# zinc-flow-csharp — next up

**Status 2026-04-15:** errors-as-values cleanup shipped (`4a1c11a`).
Pausing active csharp dev to let the current build run in anger and
accumulate feedback. Pick up from here when UI approval lands OR when
a concrete gap surfaces from running.

## Gating signal

Waiting on UI approval to commit to Phase 2f (fleet UI). Once approved
→ start with Phase 2f Phase 0 (worker-API prep). Until then, smaller
standalone items are the backfill.

## Ranked options

### 1. Phase 2f Phase 0 — worker-API prep (blocks real UI work)
Estimate: ~1 week focused. Unblocks Blazor WASM sibling project.

Touch list (from `docs/design-fleet-ui.md` + `~/.claude/plans/kind-orbiting-moth.md`):
- `ConfigLoader` with layered overlays (`config.yaml` ← `config.local.yaml` ← `secrets.yaml`)
- `YamlEmitter` (round-trip write-back)
- `ProcessorInfo.Version` (versioned processor types `type: Foo@1.2.0`)
- `/api/identity` endpoint (worker self-describe for discovery)
- `UIRegistrationProvider` (opt-in self-register to a UI host)
- Mutation API extensions: `PUT /processors/{n}/config`, `POST /flow/save`
- `GET /api/provenance/failures` (failure-queue inspector backend)
- `GET /api/overlays` (layered-config introspection)
- Optional `VersionControlProvider` (git commit/push from the worker)
- ~40 new test assertions

### 2. Phase 2d closeout — custom processor loading
Estimate: ~2-3 days. Clears the Phase 2d checklist.

- Register processors shipped in separate DLLs, not baked into main binary
- AOT-compatible path required (per feedback_aot_rules memory)
- Load at startup from a configured directory; reject unknown types at LoadFlow time via the existing ConfigException pipeline

### 3. Phase 2e closeout — schema persistence
Estimate: ~1 day. Small focused win.

- Embedded registry currently loses runtime registrations on restart
- Add optional file-backed store (JSON on disk, flushed on write)
- Config-loaded schemas still come back from `schemas:` section as today

## Not worth starting without approval

- Full Phase 2f Phase 1 (Blazor WASM SPA) — blocked on Phase 0
- Phase 2f Phase 2+3 (multi-worker, git integration) — blocked on Phase 1
- Dashboard.html incremental upgrades (provenance panel, source start/stop) — would be thrown away once Blazor UI lands; only worth doing if fleet UI is rejected

## Deferred-until-concrete-need (no action)

- Snappy OCF codec
- Confluent wire format (needs a Kafka source/sink first)
- Apache Parquet support (row-oriented flow model doesn't benefit)

## Re-entry checklist

When picking back up:
1. Re-read this file
2. Check `git log -20` since `4cd057a` for new work
3. Run `/home/vrjoshi/proj/zinc/zinc-csharp/build-tool/zinc-csharp test` — baseline is 965 passing, 0 failing
4. If UI approval landed → start Phase 2f Phase 0 with the touch list above
5. Otherwise pick option 2 or 3
