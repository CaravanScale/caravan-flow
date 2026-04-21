# Caravan Flow Roadmap

---

## What Works Now

**caravan-flow** is a fully functional standalone data flow engine. You can use it today for:

- Config-driven processor pipelines (YAML, map-keyed, NiFi-style connections)
- HTTP ingest and file-based ingest with direct pipeline execution
- Failure routing via "failure" connections in the DAG
- Backpressure (semaphore-gated concurrent executions, 503 on overload)
- Runtime lifecycle (enable/disable processors/providers, dependency cascade)
- Hot reload (atomic pipeline graph swap on config.yaml change)
- Prometheus /metrics, provenance tracking, structured logging
- Graceful shutdown on SIGTERM/SIGINT

**Three runtimes:**
- **Go** — 11MB static binary, 599K ff/s, zero deps. Edge, embedded, performance-critical.
- **C# .NET 10** — 27MB AOT binary, 2M+ ff/s, zero GC during execution. Maximum throughput, .NET ecosystems. 925 tests pass under both JIT and AOT, zero analyzer warnings.
- **Python 3.14t** — 14MB native binary, 95K ff/s, pandas/numpy/sklearn integration. Python orgs.

---

## Phase 1 — MVP (Local Dev) ✓

- [x] FlowFile + Content sealed type (Raw, Records, Claim)
- [x] ProcessorResult (Single, Multiple, Routed, Dropped, Failure)
- [x] Avro GenericRecord, JSON serde, ContentStore + offload
- [x] NiFi FlowFile V3 binary serde
- [x] 5 built-in processors (AddAttribute, Log, FileSink, JsonToRecords, RecordsToJson)
- [x] ProcessorRegistry + factory pattern
- [x] Predicate routing engine (EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, AND/OR)
- [x] HTTP source + delivery, management API (read + mutations)

## Phase 1.5 — Flow Engine ✓

- [x] Provider lifecycle (ENABLED/DISABLED), ScopedContext, dependency cascade
- [x] Map-keyed YAML config, graceful shutdown
- [x] **41 tests via `caravan test`** — core (8) + processors (7) + routing (7) + fabric/queue/DLQ (9) + scenarios (10). Real `go test` integration, `stdlib.asserts` helpers, exit-code-honest failures. (Was 30 tests / 137 assertions under the old `run_tests.sh` src/main.zn-swap hack — retired.)
- [x] caravan-flow-python port — 129 assertions, DataFrame processors
- [x] caravan-flow-csharp port — 149 assertions, ThreadStatic pools, ArrayPool, ref-counted Content, 2M+ ff/s AOT

## Phase 0 — Caravan compiler gap closure ✓

Prerequisite for porting caravan-flow's processor set to the Caravan language (re-establishing parity with the csharp reference). Audit at [docs/caravan-compiler-audit.md](docs/caravan-compiler-audit.md). Nine compiler tickets (ZCA-01 through ZCA-09) resolved across 10 commits in the [caravan](https://github.com/CaravanScale/caravan) repo:

- [x] `[imports]` alias resolution in flat-src projects (ZCA-01 + ZCA-02)
- [x] Nested user-generic type args — `Box<List<int>>` (ZCA-04)
- [x] Same-package `Map<K, UserClass>` pointer codegen (ZCA-05)
- [x] Compile-time exhaustive `match` enforcement on sealed types (ZCA-03)
- [x] `_v` unused-var fix for discard/wildcard case arms (ZCA-06)
- [x] Zero-param lambda parse `() -> { body }` (ZCA-07)
- [x] `caravan test` command + `test "name" { body }` syntax + `stdlib/asserts` module (ZCA-08)
- [x] `fmt.Errorf` with constant format string for `Error("...${interp}")` — go-vet clean (ZCA-09)
- [x] Unified `[deps]` + `[replace]` in `caravan.toml` (alias-keyed, no duplication with `[imports]`)

Remaining probes (D closure-bridge, E spawn/channels, M testing) all passed; no additional compiler gaps for MVP.

---

## Phase 2 — Useful Standalone

Make caravan-flow useful for real workloads without requiring NATS or K8s. A single binary that can ingest, transform, and deliver data.

### Phase 2a: Connectors ✓ (C#)
- [x] ConnectorSource interface (start/stop/isRunning lifecycle)
- [x] ListenHTTP (standalone server on dedicated port)
- [x] PutHTTP processor — POST flowfiles to downstream HTTP endpoints
- [x] PutFile processor — write to disk with configurable naming
- [x] GetFile source — watch a directory, ingest new files, move to .processed
- [x] PutStdout processor — write to stdout (text/hex/attrs format)
- [x] Connector lifecycle API (/api/sources, start/stop endpoints)

### Phase 2b: Observability ✓ (C#)
- [x] Prometheus /metrics endpoint (processed count, per-processor stats, source status, uptime)
- [x] Structured JSON logging option (LoggingProvider with json/text modes)
- [x] FlowFile provenance tracking (per-processor lifecycle events)
- [x] Health checks with connector status
- [x] /api/flow endpoint returns full DAG (dashboard-ready)

### Phase 2c: Hardening ✓ (C#)
- [x] Config validation at startup (missing processors, broken connections, unknown types)
- [x] Content store cleanup — periodic sweep, configurable threshold + interval
- [x] Max-hop cycle protection — runtime detection for routing loops (default 50, configurable)
- [x] DAG validator — cycle detection, unreachable processor detection, entry-point computation

### Phase 2d: Developer experience (C#)
- [x] Hot reload — watch config.yaml, atomic pipeline graph swap on change
- [x] NiFi-style connections replaced IRS global routing
- [x] Direct pipeline executor (no inter-stage queues, Apache Camel-style)
- [x] RouteOnAttribute processor — conditional branching using predicate engine
- [x] Read-only management dashboard — single-file dashboard.html served at `/`, dagre DAG layout, live processor stats
- [x] AOT hardening — source-generated JsonContext + Utf8JsonWriter for open shapes, [FromBody] parameter binding for mutation handlers, zero analyzer warnings
- [x] `test --aot` mode in caravan-csharp build tool — publishes test project as Native AOT binary for nightly CI
- [x] `caravan-flow validate` — pre-flight config check (parses YAML, builds registry, constructs every processor against a synthetic context, runs DAG validation). Exit 0 valid, 1 errors, 2 usage. (See Phase 2e for full description.)
- [x] FlowFile V3 boundary completion — GetFile sniffs V3 magic, PutFile/PutStdout `format: v3`, ListenHTTP accepts `application/flowfile-v3`, new `PackageFlowFileV3` / `UnpackageFlowFileV3` processors.
- [ ] Custom processor loading — register processors from external packages

### Phase 2e: Avro fidelity & expression engine (C#)
- [x] **Avro Object Container File (OCF)** — read real `.avro` files end-to-end. Magic bytes, Avro-encoded metadata map, 16-byte sync marker, arbitrary block count, null + deflate codecs. `ConvertOCFToRecord` + `ConvertRecordToOCF` processors with embedded JSON schema.
- [x] **Avro JSON schema parse/emit** — `AvroSchemaJson` round-trips primitives, nullable-primitive unions (`["null", T]`), and logical-type annotations.
- [x] **Logical types** — TimestampMillis, TimestampMicros, Date, TimeMillis, TimeMicros, Uuid, Decimal (precision/scale). Underlying primitive storage preserved for byte fidelity; LogicalTypeHelpers converts to/from DateTime/DateOnly/TimeOnly/Guid/decimal.
- [x] **Typed expression engine** — hand-rolled tokenizer + shunting-yard + stack VM. Tagged EvalValue (Long/Double/Bool/String/Null) with type promotion. Operators: `+ - * / %`, `== != < > <= >=`, `&& || !`, parens, unary minus. Functions: upper/lower/trim/length/substring/replace/concat/contains/startsWith/endsWith/coalesce/if/int/long/double/string/bool/abs/min/max/floor/ceil/round/pow/sqrt/isNull/isEmpty.
- [x] **TransformRecord `compute:` directive** — typed expression evaluation against record fields. Output schema preserves original FieldType for unmodified fields and infers from first record for new fields. Chained computes see prior writes.
- [x] **Nested field paths** — dotted access (`user.profile.name`) in QueryRecord, ExtractRecordField, RecordValueResolver, DictValueResolver. `RecordHelpers.GetByPath/SetByPath` walks GenericRecord and `Dictionary<string, object?>` values.
- [x] **`caravan-flow validate` subcommand** — pre-flight config check that catches unknown types, missing config keys, malformed regex, dangling connections, and DAG cycles. Exit 0 valid, 1 errors, 2 usage. Wraps the existing structural ConfigValidator with factory-construction probing.
- [x] **Avro schema evolution** — `SchemaResolver` implements Avro 1.11 resolution: int→long/float/double, long→float/double, float→double, string↔bytes promotion; reader-only fields filled from defaults; writer-only fields dropped. `ConvertOCFToRecord` accepts `reader_schema` (inline JSON) or `reader_schema_subject` (registry-backed).
- [x] **Zstd OCF codec** — `zstandard` codec via ZstdSharp.Port (pure managed, AOT-safe). null + deflate + zstandard supported; codec name follows the Avro spec ("zstandard", not "zstd").
- [x] **Embedded schema registry (airgapped)** — in-process `EmbeddedSchemaRegistry` is the sole backend. Three population paths: `schemas:` config section (pre-load + hot reload), Confluent-shape REST endpoints under `/api/schema-registry/*`, and auto-capture from incoming OCF files via `ConvertOCFToRecord`'s `auto_register_subject`. Identical-schema dedup means re-reading known files is free; new schemas become new versions automatically.
- [ ] **Schema persistence to disk** — embedded registry is in-memory only; runtime registrations are lost on restart (config-loaded ones come back). Add an optional file-backed store for persistence.
- [ ] **Snappy OCF codec** — least common in modern Avro; deferred until a concrete user needs it.
- [ ] **Confluent wire format** — 1-byte magic + 4-byte schema ID prefix on Kafka messages. Different from OCF (which embeds the full schema). Defer until we have a Kafka source/sink.
- [ ] **Apache Parquet support** — deferred; row-oriented flow model doesn't benefit from columnar storage. Revisit for one-shot batch ingest sources only.

### Phase 2f: UI (React)

React UI (`caravan-flow-ui-web`) has shipped phases 1 / 2a / 2b / 2c / 2d (Graph CRUD + observability views). Multi-worker management is addressed by the operator + aggregator split in Phase 2g below.

Shipped UI slices:
- [x] **slice 1** (`1d75420`) — React + Vite + React Flow scaffold
- [x] **2a** (`57874e4`) — editable drawer + Save-to-config button
- [x] **2b** (`9d40917`) — add-processor dialog + empty-canvas CTA
- [x] **2c** (`19b1016`) — edge drawer for connection removal
- [x] **2d** (`d4609d7`) — Errors / Provenance / Lineage / Metrics / Settings pages real

Remaining UI work that's independent of the fleet design:
- [ ] **2e** — per-user layout persistence (drag positions, localStorage keyed by flow-identity hash; never in config)
- [ ] **2f** — NiFi-style status + validation on node cards (running/stopped/invalid icon, inline config-error tooltip, Save button disabled when invalid)
- [ ] **2g** — per-node context menu (right-click start/stop/configure/delete, keyboard Enter/Del)
- [ ] **2h** — global flow controls (Stop all sources / Start all sources / Drain) in app-shell header

### Phase 2g: K8s operator + multi-worker deployment (DESIGNED)

Three deployment shapes behind one HTTP contract: single-worker standalone (unchanged), multi-worker classic (aggregator + N headless workers, non-k8s), and k8s fleet (operator + aggregator + Flow CRD). Closes the gap to NiFi clustered-mode UX: declarative fleet management, rolling updates, primary-node scheduling, drain, config validation, GitOps via ArgoCD/Flux against the Flow CRD.

**Full design:** [`docs/design-k8s-operator.md`](docs/design-k8s-operator.md). Covers CRD schemas (Flow, FlowCluster), aggregator API contract, worker headless-mode delta, RBAC, packaging (Helm + Kustomize), migration paths, testing strategy.

Phases (each is commit-sized and backward-compatible):

- [ ] **A. Contract endpoints on worker** — `GET /api/identity`, `GET /api/cluster`, `GET /api/cluster/events`. UI reads identity on boot to learn role. Ships in the next worker release, no new deployment shape.
- [ ] **B. Headless mode on worker** — `--mode=headless` + `CARAVAN_MODE` env var. Skip wwwroot, disable VC provider, 405 on `/api/flow/save`, mounted ConfigMap file watcher, hot reload on change.
- [ ] **C. Aggregator skeleton (classic mode)** — new `caravan-flow-aggregator-csharp/` project. Serves React bundle, implements full HTTP contract with fan-out over a static peer list, canonical config file + optional git integration. Enables docker-compose / VM multi-worker without k8s.
- [ ] **D. CRDs + operator skeleton** — new `caravan-flow-operator/` repo, Go + kubebuilder. Flow + FlowCluster CRDs, reconciler to ConfigMap + Deployment + Service, admission webhook, status subresource. E2E tests on kind.
- [ ] **E. Aggregator fleet mode** — aggregator reads from k8s instead of peer list. `/api/flow/save` does CRD PATCH. `/api/cluster/events` reads k8s Events.
- [ ] **F. Primary-node handling** — operator manages Lease per Flow, injects `CARAVAN_PRIMARY_NODE` env var. Worker respects `primaryOnly: true`. UI crown icon.
- [ ] **G. Rollout semantics** — config-only vs image-change paths, `POST /api/cluster/drain` with readiness-probe 503, rolling update honors `FlowCluster.spec.rollout`.
- [ ] **H. UI polish for cluster view** — worker selector in app shell, reconcile banner, per-node mini-bar stats on each processor card, cluster events panel, inline validation warnings driven by same validator the operator runs.
- [ ] **I. Packaging + docs** — Helm chart under `charts/caravan-flow/`, Kustomize bases under `deploy/kustomize/`, `caravan-flow migrate-to-crd` CLI, getting-started per mode, operator runbook.
- [ ] **J. GitOps integration** — `/api/vc/status` reads ArgoCD `Application` or Flux `Kustomization` state for the Flow resource. Example manifests in `docs/examples/`.

**Open questions (flagged in design doc):** operator language (Go vs C# — leaning Go for k8s-ecosystem reasons), primary-node election strategy (Lease vs StatefulSet ordinal — leaning Lease), hot-reload aggressiveness, aggregator HA replicas.

---

## Phase 2 — Caravan/Go port catch-up

The C# port has shipped Phase 2a-2e; the Caravan/Go port is behind. Ten concrete gaps, mapped to Go libraries that replace code C# had to hand-roll. Binary-size target: 8-9 MB stripped (7.3 MB today).

Caravan's direct-Go-import model (`import <pkg>`) means every pure-Go library is fair game. We don't get that in C# AOT without wrestling reflection/trimming.

### Priority 1 — production blockers

- [ ] **Avro OCF + schema registry** — use `github.com/hamba/avro/v2`.
  C# hand-rolled 1050 lines across AvroBinary + AvroOCF + AvroSchema + SchemaResolver + EmbeddedSchemaRegistry. `hamba/avro` ships OCF (null/deflate/snappy/zstd), binary encoding, schema JSON parse/emit, logical types (timestamp/date/decimal/uuid), and schema resolution/evolution as a library. Land the 4 processors: `ConvertAvroToRecord`, `ConvertRecordToAvro`, `ConvertOCFToRecord`, `ConvertRecordToOCF`. **Est. 300-500 lines of Caravan (vs 1050 C#).**

- [ ] **Expression engine** — use `github.com/expr-lang/expr`.
  C# hand-rolled 740 lines (tokenizer + shunting-yard + stack VM + 30+ functions). `expr-lang/expr` gives us bytecode compilation, sandboxing, type checks for free; we register the NiFi-flavored function set (upper/lower/trim/coalesce/substring/…) as the eval env. Unblocks:
  - `EvaluateExpression` processor (entirely missing)
  - `TransformRecord.compute` op (currently silently skipped — see `src/processors/transform_record.zn:19`)
  **Est. 150-200 lines of Caravan (vs 740 C#).**

### Priority 2 — core processor gaps

- [ ] **PutHTTP processor** — stdlib `net/http`. POST flowfiles, header forwarding, 429 backpressure, V3 content-type support. ~100 lines.
- [ ] **GetFile source** — stdlib `filepath` + `github.com/fsnotify/fsnotify`. Watch a directory, ingest new files, move to `.processed/`. ~150 lines.
- [ ] **PackageFlowFileV3 / UnpackageFlowFileV3 processors** — wrap the existing V3 serde (already in `core/binary.zn`) as processors. ~60 lines each.
- [ ] **CSV codec + processors** — stdlib `encoding/csv` is sufficient. `ConvertCSVToRecord` / `ConvertRecordToCSV`. ~150 lines total.

### Priority 3 — operational maturity

- [ ] **Prometheus `/metrics` endpoint** — `github.com/prometheus/client_golang`. Per-processor counters, source status, uptime. Replaces C#'s 286-line `Fabric/Metrics.cs` with a few counter registrations + `promhttp.Handler()`. ~80 lines.
- [ ] **`caravan-flow validate` subcommand** — pre-flight config check: parse YAML, build registry, construct every processor against a synthetic `ScopedContext`, run DAG validation. Exit 0 valid / 1 errors / 2 usage. ~130 lines.
- [ ] **DAG cycle + unreachable-processor detection** — reuses the data the validator collects; flagged as separate deliverable since it can run at fabric startup too. ~60 lines on top of validate.

### Priority 4 — nice to have

- [ ] **QueryRecord processor** — SQL-like filter over records. Reuses the expr env from Priority 1. ~80 lines.
- [ ] **Confluent Schema Registry REST client** — `github.com/twmb/franz-go/pkg/sr` (pure-Go subpackage). Defer until Kafka phase.

---

### Go libraries the C# port couldn't use

caravan-flow-csharp had to hand-roll infrastructure that's a library in most ecosystems, because .NET AOT + trimming aggressively drops reflection-backed code and many popular .NET libs (Apache.Avro, Roslyn scripting, …) either don't support AOT or balloon the binary. Caravan/Go inherits Go's ecosystem directly:

| Concern | C# approach | Go library | Lines saved |
|---|---|---|---|
| Avro + OCF + schema registry | AvroBinary + AvroOCF + AvroSchema + SchemaResolver (1050 lines, hand-rolled) | `hamba/avro/v2` | ~700 |
| Expression engine | ExpressionEngine.cs (740 lines, hand-rolled VM) | `expr-lang/expr` | ~550 |
| Prometheus metrics | Metrics.cs (286 lines) | `prometheus/client_golang` | ~200 |
| Zstd + snappy codecs | ZstdSharp.Port + glue | `klauspost/compress` | ~200 |
| File watching | custom poller | `fsnotify/fsnotify` | ~100 |
| **Total savings** | | | **~1750** |

The Caravan/Go port lands closer to parity with ~1000 lines of Caravan because the codec/VM/metrics primitives are in the libraries. Binary-size impact after strip: ~3-4 MB combined, keeping total under 9 MB — **roughly half the C# AOT size**.

### Attack order

1. **Avro** — unblocks 4 processors + schema registry. Biggest-impact single PR.
2. **Expression engine** — unblocks `TransformRecord.compute` and `EvaluateExpression` in one go.
3. **PutHTTP + GetFile** — connector parity; real pipelines work end-to-end.
4. **V3 package/unpackage + CSV** — boundary-format completeness.
5. **Metrics + validate** — ops maturity.
6. **QueryRecord** — reuses #2, easy finish.

---

## Phase 3 — Multi-Instance

Connect multiple caravan-flow instances into a distributed flow graph.

### Phase 3a: NATS messaging
- [ ] PutNats processor (serialize V3, publish to subject)
- [ ] GetNats source connector (subscribe, unpack V3, feed Fabric)
- [ ] NATS auth (credentials, TLS)
- [ ] Add nats.go dependency
- [ ] Integration tests with embedded NATS
- [ ] Example: two caravan-flow instances connected via NATS

### Phase 3b: K8s operator (caravan-flow-operator, separate repo)
- [ ] Scaffold with kubebuilder
- [ ] ProcessorGroup CRD + controller (deploy caravan-flow pods from CRD spec)
- [ ] Config generation (CRD spec → config.yaml ConfigMap)
- [ ] Flow CRD + controller (global topology, cross-group wiring)
- [ ] Connection validation (PutNats subject matches GetNats subject)
- [ ] Status aggregation across groups
- [ ] E2e test with kind cluster

### Phase 3c: Scaling
- [ ] Auto-scaling based on consumer lag (operator-driven)
- [ ] Partitioned NATS subjects for parallel processing
- [ ] Sticky routing — same key always routes to same processor instance

---

## Phase 4 — Ecosystem

Additional connectors, advanced features, developer tools.

### Connectors
- [ ] PutKafka / GetKafka
- [ ] PutSQS / GetSQS
- [ ] PutS3 / GetS3
- [ ] PutPostgres / GetPostgres (CDC)
- [ ] PutElasticsearch

### Advanced
- [ ] OpenTelemetry tracing (FlowFile attributes carry trace context)
- [ ] Schema registry for Avro schemas
- [ ] Content-based routing (route on FlowFile content, not just attributes)
- [ ] Windowed aggregation processor (tumbling/sliding windows)
- [ ] Join processor (merge two streams by key)

### Tools
- [ ] TUI dashboard (terminal UI with live processor stats)
- [ ] Web UI — flow graph visualization, drag-and-drop processor wiring
- [ ] Provenance viewer — trace a FlowFile's path through the graph
- [ ] **NiFi-style dynamic layout, per-user persistence** — currently
      `caravan-flow-ui-web` forces dagre auto-layout (`nodesDraggable=false`)
      because operators shouldn't spend time on cosmetic box-moving.
      For complex graphs that reasoning benefits from stable spatial
      memory, enable drag + persist positions **per-user** (localStorage
      first, keyed by a flow identity hash; later a user-scoped server
      endpoint like `PUT /api/users/me/layouts/{flowId}`). **Never**
      write positions into `config.yaml` — the k8s-replica-consistency
      story depends on `config.yaml` holding logical topology only.
      See `memory/project_ui_layout_is_ephemeral.md` for the design
      rule and render pipeline (topology → dagre seed → apply stored
      overrides → React Flow).

---

## Phase 5 — Enterprise

- [ ] Role-based access control on management API
- [ ] Audit logging (who changed what, when)
- [ ] Multi-tenant flow isolation
- [ ] SLA monitoring and alerting
- [ ] Low-code web UI for flow authoring
