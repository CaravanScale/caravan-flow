# zinc-flow-crystal

Crystal 1.20 port of `zinc-flow-csharp` — evaluating Crystal's macro
system + `preview_mt` runtime for a NiFi-style processor fabric.

## What's here

**32 processors — full parity with `zinc-flow-csharp`** — wired
through a compile-time macro registry (one source of truth per param,
no reflection, no hand-written factories):

- Attribute: `UpdateAttribute`, `LogAttribute`, `FilterAttribute`
- Text: `SplitText` (wizard), `ReplaceText`, `ExtractText`
- Conversion: `ConvertJSONToRecord`, `ConvertRecordToJSON`,
  `ConvertCSVToRecord` (wizard: RecordFields), `ConvertRecordToCSV`,
  `ConvertAvroToRecord` (wizard), `ConvertRecordToAvro` (wizard),
  `ConvertOCFToRecord`, `ConvertRecordToOCF` (wizard)
- Record: `SplitRecord`, `ExtractRecordField` (wizard),
  `QueryRecord` (wizard — JSONPath filter via sibling `jsonpath` shard)
- Transform: `EvaluateExpression` (wizard), `UpdateRecord` (wizard),
  `TransformRecord` (wizard), `RouteRecord` (wizard)
- Routing: `RouteOnAttribute` (wizard)
- Sink: `PutStdout`, `PutFile`, `PutHTTP` (POST with retries + V3 framing)
- Source: `GenerateFlowFile`, `GetFile`, `ListenHTTP` (own HTTP server)
- Utility: `CompressContent`, `DecompressContent` (gzip + zstd via FFI)
- V3: `PackageFlowFileV3`, `UnpackageFlowFileV3` (NiFi wire format)

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

**HTTP management API** matching the zinc-csharp shape — the React
UI in `zinc-flow-ui-web/` drives this worker same-origin:

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
  CRYSTAL_WORKERS=4 ./bin/zinc-flow config.yml
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
make image       # also builds zinc-flow-crystal:latest (FROM scratch)
```

## Config

`config.yml`:

```yaml
port: 9092
tick_interval_ms: 1000
ui_root: "../zinc-flow-csharp/ZincFlow/wwwroot"  # optional
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

## Processor parity: done

All 32 processors from `zinc-flow-csharp` are ported. Four of the
"week-each" library gaps closed in the process:

- ✅ **Zstd** via an FFI binding (`CompressContent`/`DecompressContent`)
- ✅ **Avro** binary + OCF container via the new `crystal-avro` sibling shard
- ✅ **JSONPath** (RFC-9535 subset) via the new `crystal-jsonpath` sibling shard
- ✅ **NiFi V3 framing** via `src/flowfile_v3.cr`
- ✅ **HTTP source + sink** via `ListenHTTP` and `PutHTTP` on stdlib `HTTP::Server` / `HTTP::Client`

Remaining non-processor gaps (infrastructure, not parity-blocking):

- **Git integration** (`/api/vc/status` + `/api/flow/save` with commit+push)
- **Config overlays** (base ← local ← env)
- **Schema registry client** (Confluent REST for `ConvertOCFToRecord`'s `readerSchemaSubject` lookup)
- **Schema evolution resolver** (writer-schema → reader-schema projection; crystal-avro v0.3)

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
