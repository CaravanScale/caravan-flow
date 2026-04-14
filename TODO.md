# Zinc Flow Roadmap

---

## What Works Now

**zinc-flow** is a fully functional standalone data flow engine. You can use it today for:

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
- [x] **41 tests via `zinc test`** — core (8) + processors (7) + routing (7) + fabric/queue/DLQ (9) + scenarios (10). Real `go test` integration, `stdlib.asserts` helpers, exit-code-honest failures. (Was 30 tests / 137 assertions under the old `run_tests.sh` src/main.zn-swap hack — retired.)
- [x] zinc-flow-python port — 129 assertions, DataFrame processors
- [x] zinc-flow-csharp port — 149 assertions, ThreadStatic pools, ArrayPool, ref-counted Content, 2M+ ff/s AOT

## Phase 0 — Zinc compiler gap closure ✓

Prerequisite for porting zinc-flow's processor set to the Zinc language (re-establishing parity with the csharp reference). Audit at [docs/zinc-compiler-audit.md](docs/zinc-compiler-audit.md). Nine compiler tickets (ZCA-01 through ZCA-09) resolved across 10 commits in the [zinc](https://github.com/ZincScale/zinc) repo:

- [x] `[imports]` alias resolution in flat-src projects (ZCA-01 + ZCA-02)
- [x] Nested user-generic type args — `Box<List<int>>` (ZCA-04)
- [x] Same-package `Map<K, UserClass>` pointer codegen (ZCA-05)
- [x] Compile-time exhaustive `match` enforcement on sealed types (ZCA-03)
- [x] `_v` unused-var fix for discard/wildcard case arms (ZCA-06)
- [x] Zero-param lambda parse `() -> { body }` (ZCA-07)
- [x] `zinc test` command + `test "name" { body }` syntax + `stdlib/asserts` module (ZCA-08)
- [x] `fmt.Errorf` with constant format string for `Error("...${interp}")` — go-vet clean (ZCA-09)
- [x] Unified `[deps]` + `[replace]` in `zinc.toml` (alias-keyed, no duplication with `[imports]`)

Remaining probes (D closure-bridge, E spawn/channels, M testing) all passed; no additional compiler gaps for MVP.

---

## Phase 2 — Useful Standalone

Make zinc-flow useful for real workloads without requiring NATS or K8s. A single binary that can ingest, transform, and deliver data.

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
- [x] `test --aot` mode in zinc-csharp build tool — publishes test project as Native AOT binary for nightly CI
- [x] `zinc-flow validate` — pre-flight config check (parses YAML, builds registry, constructs every processor against a synthetic context, runs DAG validation). Exit 0 valid, 1 errors, 2 usage. (See Phase 2e for full description.)
- [x] FlowFile V3 boundary completion — GetFile sniffs V3 magic, PutFile/PutStdout `format: v3`, ListenHTTP accepts `application/flowfile-v3`, new `PackageFlowFileV3` / `UnpackageFlowFileV3` processors.
- [ ] Custom processor loading — register processors from external packages

### Phase 2e: Avro fidelity & expression engine (C#)
- [x] **Avro Object Container File (OCF)** — read real `.avro` files end-to-end. Magic bytes, Avro-encoded metadata map, 16-byte sync marker, arbitrary block count, null + deflate codecs. `ConvertOCFToRecord` + `ConvertRecordToOCF` processors with embedded JSON schema.
- [x] **Avro JSON schema parse/emit** — `AvroSchemaJson` round-trips primitives, nullable-primitive unions (`["null", T]`), and logical-type annotations.
- [x] **Logical types** — TimestampMillis, TimestampMicros, Date, TimeMillis, TimeMicros, Uuid, Decimal (precision/scale). Underlying primitive storage preserved for byte fidelity; LogicalTypeHelpers converts to/from DateTime/DateOnly/TimeOnly/Guid/decimal.
- [x] **Typed expression engine** — hand-rolled tokenizer + shunting-yard + stack VM. Tagged EvalValue (Long/Double/Bool/String/Null) with type promotion. Operators: `+ - * / %`, `== != < > <= >=`, `&& || !`, parens, unary minus. Functions: upper/lower/trim/length/substring/replace/concat/contains/startsWith/endsWith/coalesce/if/int/long/double/string/bool/abs/min/max/floor/ceil/round/pow/sqrt/isNull/isEmpty.
- [x] **TransformRecord `compute:` directive** — typed expression evaluation against record fields. Output schema preserves original FieldType for unmodified fields and infers from first record for new fields. Chained computes see prior writes.
- [x] **Nested field paths** — dotted access (`user.profile.name`) in QueryRecord, ExtractRecordField, RecordValueResolver, DictValueResolver. `RecordHelpers.GetByPath/SetByPath` walks GenericRecord and `Dictionary<string, object?>` values.
- [x] **`zinc-flow validate` subcommand** — pre-flight config check that catches unknown types, missing config keys, malformed regex, dangling connections, and DAG cycles. Exit 0 valid, 1 errors, 2 usage. Wraps the existing structural ConfigValidator with factory-construction probing.
- [x] **Avro schema evolution** — `SchemaResolver` implements Avro 1.11 resolution: int→long/float/double, long→float/double, float→double, string↔bytes promotion; reader-only fields filled from defaults; writer-only fields dropped. `ConvertOCFToRecord` accepts `reader_schema` (inline JSON) or `reader_schema_subject` (registry-backed).
- [x] **Zstd OCF codec** — `zstandard` codec via ZstdSharp.Port (pure managed, AOT-safe). null + deflate + zstandard supported; codec name follows the Avro spec ("zstandard", not "zstd").
- [x] **Embedded schema registry (airgapped)** — in-process `EmbeddedSchemaRegistry` is the sole backend. Three population paths: `schemas:` config section (pre-load + hot reload), Confluent-shape REST endpoints under `/api/schema-registry/*`, and auto-capture from incoming OCF files via `ConvertOCFToRecord`'s `auto_register_subject`. Identical-schema dedup means re-reading known files is free; new schemas become new versions automatically.
- [ ] **Schema persistence to disk** — embedded registry is in-memory only; runtime registrations are lost on restart (config-loaded ones come back). Add an optional file-backed store for persistence.
- [ ] **Snappy OCF codec** — least common in modern Avro; deferred until a concrete user needs it.
- [ ] **Confluent wire format** — 1-byte magic + 4-byte schema ID prefix on Kafka messages. Different from OCF (which embeds the full schema). Defer until we have a Kafka source/sink.
- [ ] **Apache Parquet support** — deferred; row-oriented flow model doesn't benefit from columnar storage. Revisit for one-shot batch ingest sources only.

### Phase 2f: Fleet UI (DESIGNED, EXECUTION DEFERRED)

Headless workers + separate `zinc-flow-ui` binary that fronts a fleet via HTTP. NiFi 1 binds the UI to one node; DeltaFi has no good UI story. We do better with two independent binaries.

**Full design:** [`zinc-flow-csharp/docs/design-fleet-ui.md`](zinc-flow-csharp/docs/design-fleet-ui.md). Detailed implementation plan with file paths, test cases, and verification: `~/.claude/plans/kind-orbiting-moth.md`.

Decisions locked: Blazor WASM stack (no NPM), workflow-style fixed-slot layout (no positions in git), layered config files for secrets (`config.yaml` ← `config.local.yaml` ← `secrets.yaml`), processor versioning (`type: Foo@1.2.0`), discovery via static list + opt-in worker self-registration.

Phases:
- [ ] **0. Worker prep** — `ConfigLoader` (layered overlays), `YamlEmitter`, `ProcessorInfo.Version`, `/api/identity`, `UIRegistrationProvider`, mutation API extensions (`PUT /processors/{n}/config`, `POST /flow/save`), `GET /api/provenance/failures`, `GET /api/overlays`, optional `VersionControlProvider`. ~40 new test assertions.
- [ ] **1. UI scaffold** (single-worker mode) — new `zinc-flow-ui-csharp/` sibling project, Blazor WASM SPA, workflow-style flow visualization, provenance lineage inspector with failure queue.
- [ ] **2. Multi-worker registry** — node registry on UI (static + self-registered, heartbeat liveness), aggregated views across the fleet.
- [ ] **3. Editor + git** — insert/wire/edit processors, save back to YAML, optional commit/push.

---

## Phase 3 — Multi-Instance

Connect multiple zinc-flow instances into a distributed flow graph.

### Phase 3a: NATS messaging
- [ ] PutNats processor (serialize V3, publish to subject)
- [ ] GetNats source connector (subscribe, unpack V3, feed Fabric)
- [ ] NATS auth (credentials, TLS)
- [ ] Add nats.go dependency
- [ ] Integration tests with embedded NATS
- [ ] Example: two zinc-flow instances connected via NATS

### Phase 3b: K8s operator (zinc-flow-operator, separate repo)
- [ ] Scaffold with kubebuilder
- [ ] ProcessorGroup CRD + controller (deploy zinc-flow pods from CRD spec)
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

---

## Phase 5 — Enterprise

- [ ] Role-based access control on management API
- [ ] Audit logging (who changed what, when)
- [ ] Multi-tenant flow isolation
- [ ] SLA monitoring and alerting
- [ ] Low-code web UI for flow authoring
