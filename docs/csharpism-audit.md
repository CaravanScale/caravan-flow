# csharpism audit — today's commits

Ground rule: csharp is the source of **design** (what the system does,
what components exist, what the architecture looks like), not the
source of **code idioms** (how control flow is shaped, how errors are
reported, how partial states are represented).

Zinc's design constraint: **everything that can fail must go through
the error mechanism** — `T?` return + `Error(reason)` + `or { }` at
the call site. Silent skips, silent fallbacks, and partial-construction
short-circuits are category errors.

This doc surveys every pattern in today's commits (`b8b1e19` through
`ddc9c8a`) where a csharp idiom leaked into Zinc code. Each entry
flags the site, names the idiom, and marks whether it's been fixed,
is pending a decision, or is benign.

---

## Fixed

### A1. Bare `return` in constructor body (FilterAttribute)
- **Site:** `src/processors/filter_attribute.zn:25` (pre-fix)
- **Idiom:** csharp allows `if (bad) return;` in a ctor. Partial
  object visible to caller, no failure signal.
- **Resolution:** zinc-go now rejects bare-return-in-ctor at compile
  time (`zinc-go/internal/codegen_go/codegen_types.go` +
  `examples-fail/ctor_bare_return.zn`). Site rewritten with guard-
  invert pattern. Commit: `26b21cd` (compiler) + `ddc9c8a` (site).

---

## Pending — needs design decision

### B1. `atoi`-style silent fallback parsers
- **Sites:**
  - `src/fabric/source/generate.zn:atoi` (`batch_size`,
    `poll_interval_ms`)
  - `src/processors/extract_record_field.zn:parseIntOr`
    (`record_index`)
  - `src/processors/text_processors.zn:textParseInt` (`header_lines`)
- **Idiom:** csharp `int.TryParse(s, out var n) ? n : fallback`.
  Bad input silently becomes the default. The user sets
  `batch_size: "seven"` in YAML and gets batch_size=1 with no warning.
- **Zinc design:** `parseInt(s): int?` returning `Error("not a
  number: %s")`. Callers get an `or { fallback }` explicit-default
  form at every site — the fallback becomes a deliberate choice at
  the call site, not a hidden behavior in the parser.

### B2. `parseRoutes` silently drops malformed route entries
- **Site:** `src/processors/route_on_attribute.zn:42-63`
- **Idiom:** csharp `SplitStringOptions.RemoveEmptyEntries` +
  `if (parts.Length < 2) continue`. A misconfigured route string
  produces a processor that silently routes less traffic than the
  user expected.
- **Zinc design:** `RouteOnAttribute(spec): RouteOnAttribute?`
  factory. Parse errors abort construction and surface as a
  registry-create failure. Pipeline fails to load on typo.

### B3. `parseFields` / `parseOperations` / `parseAttributes`
- **Sites:**
  - `src/processors/extract_record_field.zn:parseFields`
  - `src/processors/transform_record.zn:parseOperations`
  - `src/fabric/source/generate.zn:parseAttributes`
- **Idiom:** same as B2 — silent-skip on any malformed `k:v` pair.

### B4. TransformRecord silently skips unknown op names
- **Site:** `src/processors/transform_record.zn:applyOne` (last
  fallthrough — including `compute` which isn't implemented)
- **Idiom:** csharp `switch { ... default: break; }`.
  `transform_record.applyOne` falls through silently on unrecognized
  ops. If a user writes `compute:foo:bar` today, the whole op is
  dropped with no signal.
- **Zinc design:** factory validates op vocabulary at config time;
  unrecognized op → `Error`. `compute` stays unimplemented but
  registering it in the "known but not implemented" set produces a
  different, clearer error ("compute op not yet implemented — pending
  expression engine port") vs. "unknown op".

### B5. ExtractRecordField silently skips missing fields
- **Site:** `src/processors/extract_record_field.zn:process` loop
- **Idiom:** csharp `if (val is not null) result = ...`. If the
  record has no field by that name, the attribute simply isn't set.
- **Zinc design decision:** this one is arguably **benign** — the
  semantic intent is "extract what's there." But the current behavior
  silently hides a config/data mismatch (user names a field that was
  renamed upstream). A `strict: true` config mode that errors on
  missing fields would be worth adding.

### B6. RouteOnAttribute fall-through to `"unmatched"`
- **Site:** `src/processors/route_on_attribute.zn:process` final
  return
- **Idiom:** csharp-style "default route." When no predicate matches,
  the FlowFile goes to relationship `"unmatched"` — silent by
  default.
- **Zinc design decision:** this is actually **load-bearing** — lots
  of NiFi configs rely on a catch-all route. But the FlowFile still
  needs a wired `unmatched:` target in the YAML, or the executor
  drops it silently. Consider: log-warn on unwired `unmatched`
  fallthrough.

### B7. Fabric.loadFlow silently skips unknown processor types
- **Site:** `src/fabric/runtime/runtime.zn:loadFlow` — `if
  !reg.has(typeName) { logging.error(...); continue }`
- **Idiom:** csharp `Console.Error.WriteLine("Unknown processor
  type: {typeName}")` + `continue`. Load "succeeds," pipeline comes
  up missing a processor, FlowFiles dead-letter or hang depending on
  where the missing processor sat in the graph.
- **Zinc design:** `loadFlow` should return a load-result with
  collected errors; any unknown type aborts the whole load. This
  also applies to a processor targeting a connection whose target
  doesn't exist (currently caught at execute-time with a log-error
  + drop).

### B8. Fabric.ingestAndExecute returns false on no entry points
- **Site:** `src/fabric/runtime/runtime.zn:ingestAndExecute` first
  guard
- **Idiom:** csharp `return false` as backpressure signal. The bool
  return IS the error channel here — the source gets told "not
  accepted." Technically this IS using an error mechanism (bool),
  not a silent drop.
- **Status:** **benign** — the IngestFn contract is
  `Fn<(FlowFile), bool>` where false means "pipeline couldn't take
  it." This is the right shape for an event-driven source.

### B9. ExecuteGraph unknown-processor drop at runtime
- **Site:** `src/fabric/runtime/runtime.zn:executeGraph` — `if
  !g.hasProcessor(item.processor) { logging.error(...); continue }`
- **Idiom:** runtime robustness pattern — keep going on bad state.
  If loadFlow validated the graph (B7), this branch should be
  unreachable and could be a panic ("graph invariant violated").
- **Zinc design:** couple with B7. Once loadFlow rejects invalid
  topologies, this guard becomes an assertion.

---

## Out of scope (zinc-go compiler)

Today's compiler commits (`b8b1e19`..`ddc9c8a`) were all
**correctness fixes** — the compiler was emitting wrong Go for
realistic Zinc code. No csharp idioms snuck in EXCEPT the short-lived
`inConstructor` hack which is reverted (see A1).

---

## Proposed execution order

1. **B1** (atoi → `parseInt(s): int?`) — localized, motivates the
   pattern before the bigger factory-refactor.
2. **B2/B3** combined — change the parse helpers to return error
   lists; factories that see errors log loudly (first fallback) or
   fail to construct (once factories return `T?`).
3. **Factory refactor** — lift `ProcessorFactory` to
   `Fn<(ScopedContext, Map<String, String>), ProcessorFn?>`.
   Registry's `create()` propagates. Fabric.loadFlow collects and
   reports.
4. **B7/B9** — tighten loadFlow validation; demote execute-time
   unknown-processor guard to an invariant check.
5. **B4** — unknown op in TransformRecord → construct-time error
   via the now-T? factory.
6. **B5/B6** — add explicit strict modes; add unwired-unmatched
   warning at load.
