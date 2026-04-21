# caravan-flow-crystal

Crystal 1.20 port of `caravan-flow-csharp` — evaluating Crystal's macro
system + `preview_mt` runtime for a NiFi-style processor fabric.

## What's here

**21 processors** wired through a compile-time macro registry — one source
of truth per param, no reflection, no hand-written factories:

- Attribute: `UpdateAttribute`, `LogAttribute`, `FilterAttribute`
- Text: `SplitText` (wizard), `ReplaceText`, `ExtractText`
- Conversion: `ConvertJSONToRecord`, `ConvertRecordToJSON`,
  `ConvertCSVToRecord` (wizard: RecordFields),
  `ConvertRecordToCSV`
- Record: `SplitRecord`, `ExtractRecordField` (wizard)
- Transform: `EvaluateExpression` (wizard), `UpdateRecord` (wizard),
  `TransformRecord` (wizard), `RouteRecord` (wizard)
- Routing: `RouteOnAttribute` (wizard)
- Sink: `PutStdout`, `PutFile`
- Source: `GenerateFlowFile`, `GetFile`

**Expression language** (`src/expression.cr`) — tree-walking interpreter
for the EL variant used by `EvaluateExpression`, `UpdateRecord`,
`RouteRecord`, and `TransformRecord:compute`. Supports arithmetic,
string concat, comparisons, boolean logic, and a functions set
(`upper`, `lower`, `trim`, `length`, `substring`, `concat`, `int`,
`double`, `isNull`, `isEmpty`, `contains`, `startsWith`, `endsWith`, `if`).

**Fabric runtime** — enabled/disabled state, per-node stats, mutation
counter + dirty flag, edge stats (for UI motion), provenance ring
(CREATE / ROUTE / FAILURE), per-node output sample ring (for the Peek
tab), thread-safe via `Atomic` + `Mutex`.

**HTTP management API** matching the caravan-csharp shape — the React
UI in `caravan-flow-ui-web/` drives this worker same-origin:

- `GET /api/registry`, `GET /api/flow`, `GET /api/processor-stats`
- `POST /api/processors/add`, `DELETE /api/processors/remove`
- `POST /api/processors/enable`, `/disable`
- `PUT /api/processors/{name}/config`
- `POST/DELETE/PUT /api/connections`
- `POST /api/processors/{name}/stats/reset`
- `GET /api/sources`, `POST /api/sources/start`, `/stop`, `/add`
- `GET /api/provenance[?n]`, `/failures[?n]`, `/{id}`
- `GET /api/processors/{name}/samples` (last 5 emitted flowfiles)
- `GET /api/edge-stats`
- `GET /api/layout`, `POST /api/layout`
- `POST /api/expression/parse` (live preview for the UI's builder)
- `GET /metrics` (Prometheus text exposition)
- `GET /api/flow/status`, `POST /api/flow/save`, `POST /api/reload`
- `GET /api/overlays`, `GET /api/vc/status` (stubbed — see below)
- UI static fallback (`ui_root` in config.yml)

## Build

### Quick dev loop (host Crystal via brew)

Requires brew + pcre from the earlier install, Crystal on PATH via
`brew sh`:

```
brew sh -c 'make build'       # debug, Dpreview_mt
brew sh -c 'make test'        # crystal spec
```

Run:
```
LD_LIBRARY_PATH=/home/linuxbrew/.linuxbrew/lib:/home/linuxbrew/.linuxbrew/opt/pcre/lib \
  CRYSTAL_WORKERS=4 ./bin/caravan-flow config.yml
```

### Portable static binary (what you ship)

```
make build-static
```

Spins up Alpine + Crystal 1.20 in Docker, runs specs + `--release
--static -Dpreview_mt`, extracts the 7 MB binary to `./bin/` — no
GLIBC/brew/PCRE runtime dependencies. Drop it on any Linux kernel ≥
2.6.32 (RHEL 8/9/10, Alpine, scratch containers).

```
make image       # also builds caravan-flow-crystal:latest (FROM scratch)
```

## Config

`config.yml`:

```yaml
port: 9092
tick_interval_ms: 1000
ui_root: "../caravan-flow-csharp/CaravanFlow/wwwroot"  # optional
layout_path: "layout.json"                             # optional

nodes:
  - name: gen
    type: GenerateFlowFile
    config: {content: "ping", batch_size: "1"}
    routes:
      success: [tag]
  - name: tag
    type: UpdateAttribute
    config: {key: "env", value: "prod"}
```

## What's deliberately missing (Slice 4)

Status update: **Zstd, Avro, and OCF are now all shipped.** Two of
the three "week-each" library gaps are closed; the Avro work became
its own publishable shard at `../crystal-avro/` (sibling to
this directory).

Remaining gaps with scope estimates:

- **JsonPath** (`QueryRecord`) — needs an RFC-9535 parser +
  evaluator. ~3-5 days for the subset caravan uses.
- **NiFi V3 framing** (`PackageFlowFileV3`, `UnpackageFlowFileV3`) —
  custom binary format; ~2-3 days to translate from the C# impl.
- **PutHTTP / ListenHTTP** — stdlib has `HTTP::Client` and
  `HTTP::Server`; ~1-2 days each.
- **Git integration** (`/api/vc/status` + `/api/flow/save` with
  commit+push) — need a git2 shard or shell out to `git`.
- **Config overlays** (base ← local ← env) — right now we load
  `config.yml` as a single source. Overlay merging is straightforward
  once we want it.
- **Schema registry client** (`ConvertOCFToRecord` reader schema
  lookup) — the C# sibling supports a `readerSchemaSubject`
  parameter backed by Confluent REST. Left as a follow-up on
  `crystal-avro`.

The 27 processors that ARE here cover every design pattern the
registry hits: scalar params, enums, stringlists, keyvaluelists
w/expression values, multiline DSLs, source semantics, record
content, regex, compression, **Avro binary + OCF container** (via
`crystal-avro`). The parts deferred are all *library availability*
problems — none of them prove Crystal can't do this.

## Developer notes

Macros: `src/processor.cr#register` is the money-maker. Look at how
e.g. `UpdateAttribute` declares itself in ~15 lines including the
implementation — that's what earlier C# `Processors.cs` + processor
class was together. Add a processor: create `src/processors/foo.cr`,
append `require` line in `src/processors/all.cr`, done.

MT: build with `-Dpreview_mt`, run with `CRYSTAL_WORKERS=N`. The
fabric `spawn {}`s on every downstream dispatch, so with N workers
the pool concurrently drains sibling fanouts.

Known Crystal 1.20 parser quirks hit during this port — fully
minimized and documented in [`docs/bug-reports/`](./docs/bug-reports/).

- **`out` keyword shadows local variable after `return`/`next`/`break`**:
  a 4-line reproducer. Surfaces as either `expecting variable or
  instance variable after out` or a cascading `can't define def inside
  def` further down the file. See
  [`docs/bug-reports/01-out-keyword-shadows-local.md`](./docs/bug-reports/01-out-keyword-shadows-local.md).
  Workaround: rename the local, or wrap with parens (`return (out)`).

The runnable reproducers under `docs/bug-reports/reproducers/` have a
`verify.sh` that re-runs them against the current Crystal — if any
starts compiling, the bug is fixed and we should update the report.
