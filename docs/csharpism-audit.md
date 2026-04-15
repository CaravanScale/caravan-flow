# csharpism audit — today's commits

Ground rule: csharp is the source of **design** (what the system
does, what components exist, what the architecture looks like), not
the source of **code idioms** (how control flow is shaped, how
errors are reported, how partial states are represented).

Zinc's design constraint: **everything that can fail must go through
the error mechanism** — `T?` return + `Error(reason)` + `or { }` at
the call site. Silent skips, silent fallbacks, and partial-
construction short-circuits are category errors.

This doc is the exhaustive file-by-file survey of today's commits
(`ef15d0b` through `ddc9c8a` on zinc-flow; `b8b1e19` through
`26b21cd` on zinc-go). Every site where a csharp idiom leaked is
flagged with `file:line`, the specific idiom, and a proposed Zinc
design.

---

## Fixed already

### A1. Bare `return` inside constructor bodies
- **Resolution:** zinc-go now rejects at compile time via
  `checkCtorBodyNoBareReturn` in
  `zinc-go/internal/codegen_go/codegen_types.go`. Recurses into
  nested `if / for / while / match / parallel for` blocks AND into
  `or { }` handlers on Var/TupleVar/Assign/Expr/ParallelFor stmts.
  Deliberately does NOT recurse into `spawn { }` or deferred
  closures (bare return there is scoped to that closure, not the
  ctor).
- **Negative tests:** `zinc-go/examples-fail/ctor_bare_return.zn`
  (direct if-guard) + `ctor_bare_return_in_handler.zn`
  (`or { return }`). Commit: `26b21cd` + follow-up coverage
  extension.
- **Site rewrite:** `src/processors/filter_attribute.zn:24` now
  guard-inverts around the empty-spec path. Commit: `ddc9c8a`.

---

## Pending — needs design decision

Grouped by category. File:line references from the committed state.

### B. Silent-fallback int parsers (Group)
Copies of csharp's `int.TryParse(s, out var n) ? n : fallback`. Bad
input silently becomes the default; user misconfiguration hides.

- `src/fabric/source/generate.zn:205` — `atoi(s, fallback)`, called
  for `batch_size` and `poll_interval_ms`.
- `src/processors/extract_record_field.zn:125` — `parseIntOr`,
  called for `record_index`.
- `src/processors/text_processors.zn:243` — `textParseInt`, called
  for `header_lines`.

**Zinc design:** `parseInt(s): int?` returning
`Error("not a number: %s")`. Callers take an `or { fallback }` at
each site — the fallback becomes a deliberate choice at the call
site, not hidden behavior in the parser.

### C. Silent-drop malformed config parsers (Group)
Copies of csharp's `SplitStringOptions.RemoveEmptyEntries` +
`if (parts.Length < 2) continue`. Malformed config entries silently
disappear; load succeeds with a partial processor.

- `src/processors/route_on_attribute.zn:34-63` (`parseRoutes`) — doc
  at line 12-13 explicitly admits the csharpism.
- `src/processors/extract_record_field.zn:38-82` (`parseFields` +
  `addPair`) — doc at line 16-17 + 42-44 admits it.
- `src/processors/transform_record.zn:parseOperations +
  addEntry` — doc at line 197-199 admits it.
- `src/fabric/source/generate.zn:parseAttributes + addPair`
  (lines 64-99).

**Zinc design:** factories return `ProcessorFn?` /
`ConnectorSource?`. Parse errors abort construction and surface at
the registry level. Pipeline fails to load on typo.

### D. Unknown-op silent-skip (inside TransformRecord)
- `src/processors/transform_record.zn:151-200` — `applyOne` falls
  through silently on any op name it doesn't recognize, including
  `compute` which isn't implemented.
- **Zinc design:** factory validates op vocabulary at construction
  time; unrecognized op → `Error`. `compute` stays registered as
  "known but not implemented" so its error message is clearer than
  the catch-all.

### E. Unknown-operator silent-false (inside RouteOnAttribute)
- `src/processors/route_on_attribute.zn:85-114` — `evaluate` falls
  through to `return false` if `op` isn't in the known set. A route
  with `op: WEIRDOP` silently never matches.
- **Zinc design:** validate operator strings at `parseRoutes` time;
  unknown op → factory-level Error.

### F. Missing-required-config silent-empty-string (factories group)
When a config key is missing, `config["key"]` returns `""` (Go map
zero value). Factories use these without checking, constructing
processors with empty-string "config."

- `src/processors/update_attribute.zn:33` — `config["key"]` /
  `config["value"]` silently `""`.
- `src/processors/builtin.zn:100` — `AddAttributeFactory` same.
- `src/processors/builtin.zn:105, 108` — `FileSinkFactory` silent
  `config["output_dir"]`.
- `src/processors/builtin.zn:112` — `LogProcessorFactory` silent
  `config["prefix"]`.
- `src/processors/put_file.zn:91-92` — missing `output_dir` →
  empty, leading to writes at `"/filename"`.

**Zinc design:** factory validates required keys up front, returns
Error if missing. Once `ProcessorFactory` returns `ProcessorFn?`
this is a one-line check per factory.

### G. Silent defaults on known optional keys
Similar shape but for config keys that have defaults. Less severe
than F — the default is documented and intentional. Still, a silent
"I substituted a default for what you typed" when the user TYPED
something is a leak.

- `src/processors/put_stdout.zn:73-76` — default `format="raw"`.
- `src/processors/put_stdout.zn:55-58` — unknown format silently
  falls through to raw-text output.
- `src/processors/put_file.zn:60-62` — non-"v3" format silently
  treated as "raw" for the output bytes.
- `src/processors/put_file.zn:101-104` — default format="raw".
- `src/processors/builtin.zn:115-119` — JsonToRecordsFactory silent
  default `schemaName = "default"`.
- `src/processors/log_attribute.zn:41-46` — factory default
  `prefix = "flow"`. (Marginal — prefix is descriptive only.)

**Zinc design:** validate the value is in the allowed set; reject
unknown values at factory time. Defaults that are ACTUALLY set by
the user (vs. missing) should at minimum be validated.

### H. Silent pass-through on wrong content variant
Processors match `ff.content` and silently `Single(ff)` on variants
they don't handle.

- `src/processors/builtin.zn:RecordsToJson.process` (lines
  232-247) — Raw and Claim variants silently pass through unchanged
  (comment says "If content is already Raw, passes through
  unchanged" but a processor named "records to json" doing nothing
  on non-record input is surprising).
- `src/processors/extract_record_field.zn:100-105` — non-Record
  content passes through silently. Comment acknowledges this as
  intentional (pipeline keeps moving).

**Zinc design decision:** arguably benign for ExtractRecordField
(staged pipeline with optional records). For RecordsToJson it's
likely an error — the processor can't do its job, so the FlowFile
should go to a failure route.

### I. Silent-skip missing Record field (ExtractRecordField)
- `src/processors/extract_record_field.zn:92-96` — missing field
  silently omitted from attribute set. csharp behaves the same;
  doc at line 16-17 admits it.
- **Zinc design decision:** benign for soft-extract workloads; a
  `strict: true` config mode that errors on missing fields would
  cover the hard-extract case without breaking back-compat.

### J. RouteOnAttribute fall-through to `"unmatched"`
- `src/processors/route_on_attribute.zn:71` — when no predicate
  matches, FlowFile goes to relationship `"unmatched"`.
- **Zinc design decision:** load-bearing — NiFi configs rely on a
  catch-all. But if `unmatched:` isn't wired in the YAML, the
  FlowFile drops silently in the executor (pushDownstream with no
  targets returns without Failure). Proposed mitigation: loadFlow
  warns when a processor's declared routes include `unmatched` and
  no `unmatched:` connection is wired. (Or all processors get a
  reserved `unmatched` warning.)

### K. Fabric loadFlow silently skips unknown processor types
- `src/fabric/runtime/runtime.zn:108-111` — `if !reg.has(typeName)
  { logging.error + continue }`. Load "succeeds," pipeline is
  partial.
- **Zinc design:** `loadFlow` should return a load-result with
  collected errors; any unknown type aborts the whole load.

### L. ExecuteGraph unknown-processor drop at runtime
- `src/fabric/runtime/runtime.zn:217-221` — runtime-robustness
  pattern: keep going on bad state. If K is fixed, this branch is
  unreachable and could be a panic/invariant assertion.

### M. Fabric.addProcessor returns bool but factory can't fail
- `src/fabric/runtime/runtime.zn:598-624` — `reg.create()` returns
  non-optional `ProcessorFn`. Factory-internal failures (bad config,
  provider missing) can't be reported to Fabric. Tightly coupled
  to F.
- **Zinc design:** `Registry.create` returns `ProcessorFn?`;
  `addProcessor` returns `bool` OR an error-string; `loadFlow`
  collects all per-processor errors.

### N. buildScopedContextFor silently drops missing providers
- `src/fabric/runtime/runtime.zn:672-691` — `or { continue }`
  silently skips providers that a processor declared in `requires`
  but globalCtx doesn't have. Processor gets a partial scope.
- **Zinc design:** if a processor `requires` provider X and X isn't
  present, construction aborts with Error.

### O. PipelineGraph accessors return silently-bogus defaults
- `src/core/pipeline_graph.zn:103-105` — `getProcessor` "panics if
  unknown" (docstring admits). csharpism: throw vs. return
  `ProcessorFn?`.
- `src/core/pipeline_graph.zn:120-125` — `getState` returns
  DISABLED silently when name unknown.
- `src/core/pipeline_graph.zn:132-136` — `setState` silently
  no-ops on unknown name.
- `src/core/pipeline_graph.zn:168-173` — `getRequires` returns
  empty list on unknown name.

**Zinc design:** accessors return `T?` (e.g.
`getProcessor(name): ProcessorFn?`,
`getState(name): ComponentState?`). Callers get explicit failure
and can `or { }` a decision at the call site.

### P. Fabric.getProcessorType returns "unknown" string
- `src/fabric/runtime/runtime.zn:438-443` — silent sentinel
  "unknown" instead of error.
- **Zinc design:** `getProcessorType(name): String?`.

### Q. handlers.zn ignores replayAt return
- `src/fabric/api/handlers.zn:109` — `fab.replayAt(sourceProc, ff)`
  return value (bool) ignored. If replay fails (unknown processor),
  user gets 200 "replayed" response.
- `src/fabric/api/handlers.zn:123` — same pattern in
  dlqReplayAllHandler.
- **Zinc design:** check return, write 404/409 when false.

### R. handlers.zn empty `or { }` swallows unmarshal error
- `src/fabric/api/handlers.zn:279` — `json.Unmarshal(configBytes,
  rawConfig) or {}` — empty handler block silently drops the
  unmarshal error, then continues as if config was empty.
- **Zinc design:** surface as 400 response with the error message,
  OR at minimum log it.

### S. GenerateFlowFile loadGenerateSource silent skip on missing content
- `src/fabric/source/generate.zn:181-184` — `if
  !cfg.has("sources.generate.content") { return }`. A `sources.generate`
  section without `content` silently registers no source.
- **Zinc design decision:** if sources.generate exists but content
  missing → Error; if whole sources.generate is absent → fine.

### T. put_file.basename silently strips directory traversal
- `src/processors/put_file.zn:79` + basename helper at line
  112-128 — attacker sending `filename: "../../etc/passwd"` gets
  silently reduced to "passwd" (written into output_dir).
- **Zinc design decision:** reject-with-Failure when the naming
  attribute contains `/` or `..` rather than silently sanitizing.
  Silent sanitize is a security anti-pattern: attacker learns their
  payload "worked" (no error) and iterates.

---

## zinc-go compiler commits (audited)

| Commit | Files | Verdict |
|---|---|---|
| `26b21cd` | codegen_types.go | **Zinc-correct.** Replaces the prior csharpism hack. The `switch` default case in `checkCtorStmtNoBareReturn` deliberately does nothing for Stmt types that can't contain nested control flow (Break, Continue, AssertStmt, PrintStmt, etc.) — not a silent skip, a correct no-op. |
| `9caffb1` | codegen_exprs.go | **Zinc-correct.** Field-casing + lambda return-type inference — both are compiler correctness fixes. |
| `3a1d569` | codegen_resolve.go, codegen_stmts.go, codegen_types.go | **Zinc-correct.** Param-type propagation so `.keys()`/`.values()` stay typed through realistic receiver shapes. |
| `2d86ec4` | codegen_calls.go, codegen_resolve.go, codegen_stmts.go | **Zinc-correct.** Receiver-shape sweep. No idioms imported. |
| `e83cdd7` | codegen_calls.go, codegen_resolve.go | **Zinc-correct.** IndexExpr walking, typed channel recv. |
| `5178cde` | codegen_calls.go, codegen_resolve.go, codegen_stmts.go | **Zinc-correct.** Map K/V typed generation. |
| `afc4d16` | codegen_calls.go, codegen_exprs.go, codegen_resolve.go, codegen_stmts.go | **Zinc-correct.** Six identifier-vs-package precedence fixes. |
| `0865ec0` | codegen_calls.go | **Zinc-correct.** Field-name shadows subpackage. |
| `d8453bd` | codegen_stmts.go | **Zinc-correct.** `Error(...)` interpolation emission. |
| `53529ea`, `05631de`, `049b7b9`, `91eec11`, `92aa080`, `a738e0c`, `b8b1e19`, `f0c6c1d` | various | Audited, all compiler/stdlib correctness. |

**Only csharpism found in compiler:** the short-lived `inConstructor`
emission hack from an earlier iteration (now reverted by `26b21cd`
and documented in A1).

---

## Proposed execution order

1. **Group B** (atoi → `parseInt(s): int?`) — localized, motivates
   the pattern and migrates all three sites.
2. **Factory refactor** — lift `ProcessorFactory` to
   `Fn<(ScopedContext, Map<String, String>), ProcessorFn?>` and
   `SourceFactory` to `Fn<(String, Map<String, String>), ConnectorSource?>`.
   `Registry.create` propagates. Fabric.loadFlow collects errors
   from every failed factory and reports the aggregate.
3. **Group F** — once factories can fail, validate required config
   keys at load time.
4. **Groups C + D + E** — parse helpers surface Error on malformed
   input; factories propagate.
5. **Groups K + L + M + N + O + P** — tighten loadFlow validation;
   demote execute-time guards to invariant assertions. PipelineGraph
   accessors return `T?`.
6. **Groups G + H + I + J + S + T** — case-by-case decisions:
   add explicit strict modes, warn on unwired `unmatched`, reject
   rather than silently sanitize in PutFile.
7. **Groups Q + R** — handlers.zn response-path polish.

---

## Summary

- **1 fixed** (A1, bare-return-in-ctor).
- **19 pending** across 11 groups (B–T) — every one is a silent
  skip, silent fallback, or silent default that should surface
  through the error mechanism.
- **Compiler commits** clean; the one csharpism (A1) is already
  reverted.
- **Execution order** starts localized (B) and ends with
  fine-grained policy (Q/R), so we can make incremental progress
  without the big ProcessorFactory refactor blocking small wins.

---

## csharp-side parallel cleanup (status)

The groups above enumerate *Zinc-side* (`src/**/*.zn`) sites. The csharp
reference has analogous sites that were cleaned up independently under
the same design rule (every config error must surface at `LoadFlow` time
through `ConfigException` → aggregated into `AggregateException`).
Tracked here so the csharp side doesn't accumulate drift.

Closed on csharp:
- **B-analog (atoi sites):** `ConfigHelpers.ParseInt/ParseLong/ParseBool/
  ParseSingleChar/RequireOneOf` replace silent `TryParse`-with-fallback.
  `Providers.ConfigProvider.GetInt/GetBool` throw on unparseable values
  instead of silent default. `ApiHandler` `/api/provenance?n=` returns
  400 on bad int instead of silently defaulting to 50.
- **C-analog (malformed-spec continues):** RouteOnAttribute, ExtractRecordField,
  ConvertAvroToRecord.ParseFieldDefs, QueryRecord — all `continue`-on-malformed
  loops replaced with `ConfigException` throws.
- **D-analog (TransformRecord unknown op):** op vocabulary enforced at
  construction time via `_knownOps` set; unknown op → ConfigException.
- **E-analog (RouteOnAttribute unknown operator):** `ParseOperator` fallthrough
  `_ => Eq` replaced with ConfigException listing valid operators.
- **F-analog (required config keys):** UpdateAttribute now requires both
  `key` AND `value` (previously `value` was optional with empty default).
- **G-analog (unknown value in enum config):** PutStdout format →
  `RequireOneOf {attrs, raw, text, v3, hex}`. ConvertRecordToOCF codec →
  `RequireOneOf {null, deflate, zstandard}`.
- **J-analog (unwired "unmatched"):** `FlowValidator` emits a warning
  when RouteOnAttribute has no `unmatched:` connection wired.
- **B7-analog (LoadFlow aggregation):** already in place from commit
  `8d5835c`; tests now prove multiple broken entries surface in one
  pass via `ConfigErrorTests.TestLoadFlowAggregatesAllErrors`.

Deliberately left as-is on csharp (benign defaults, documented):
- `CsvRecord.CoerceValue` string fallback (data preservation priority).
- `PollingSource` non-positive interval → 1000ms floor (prevents spin).
- `LogAttribute.prefix` default (descriptive only).
- `ExpressionEngine.AsDouble/AsLong` (runtime coercion, not config).
- `TransformRecord` compute expression — bad expression is swallowed
  at construction time (existing `TestMalformedExpressionTolerated`
  documents this; flip would require deliberate design change).

Remaining on csharp:
- **L-analog (runtime invariant):** `Fabric.ExecuteGraph`'s `if
  (!g.hasProcessor)` guard could be demoted to `Debug.Assert` now that
  `LoadFlow` guarantees the invariant. Low-priority — the runtime
  defensive check is cheap.

Coverage: 17 new assertions in `ZincFlow.Tests/ConfigErrorTests.cs`
across all the above paths + a 4-error aggregation test proving
`LoadFlow` reports every problem, not just the first.
