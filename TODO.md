# Zinc Flow Roadmap

---

## What Works Now

**zinc-flow** is a fully functional standalone data flow engine. You can use it today for:

- Config-driven processor pipelines (YAML, map-keyed)
- HTTP ingest with predicate-based routing (IRS fan-out)
- Transactional delivery (claim/ack/nack, at-least-once, visibility timeout)
- Backpressure (bounded queues, 503 on overload)
- Dead letter queue with inspect/replay via API
- Runtime lifecycle (enable/disable processors/providers, dependency cascade)
- Graceful shutdown on SIGTERM/SIGINT

**Three runtimes:**
- **Go** — 11MB static binary, 599K ff/s, zero deps. Edge, embedded, performance-critical.
- **C# .NET 10** — 16MB AOT binary, 2M+ ff/s, zero GC during execution. Maximum throughput, .NET ecosystems.
- **Python 3.14t** — 14MB native binary, 95K ff/s, pandas/numpy/sklearn integration. Python orgs.

---

## Phase 1 — MVP (Local Dev) ✓

- [x] FlowFile + Content sealed type (Raw, Records, Claim)
- [x] ProcessorResult (Single, Multiple, Routed, Dropped, Failure)
- [x] Avro GenericRecord, JSON serde, ContentStore + offload
- [x] NiFi FlowFile V3 binary serde
- [x] 5 built-in processors (AddAttribute, Log, FileSink, JsonToRecords, RecordsToJson)
- [x] ProcessorRegistry + factory pattern
- [x] IRS predicate routing (EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, AND/OR)
- [x] HTTP source + delivery, management API (read + mutations)

## Phase 1.5 — Flow Engine (Transactional Delivery) ✓

- [x] Provider lifecycle (ENABLED/DRAINING/DISABLED), ScopedContext, dependency cascade
- [x] FlowQueue — transactional, bounded, visibility timeout, head-index with compaction
- [x] ProcessSession, IRS all-or-nothing fan-out, backpressure, DLQ with replay
- [x] Async processing, graceful shutdown, map-keyed YAML config
- [x] 30 tests, 137+ assertions, 10 e2e scenarios
- [x] zinc-flow-python port — 129 assertions, DataFrame processors, performance optimized
- [x] zinc-flow-csharp port — 149 assertions, ThreadStatic pools, ArrayPool, ref-counted Content, 2M+ ff/s AOT

---

## Phase 2 — Useful Standalone

Make zinc-flow useful for real workloads without requiring NATS or K8s. A single binary that can ingest, transform, and deliver data.

### Phase 2a: Connectors — get data in and out
- [ ] ConnectorSource interface (start/stop/isRunning lifecycle)
- [ ] Refactor HttpSource to implement ConnectorSource
- [ ] PutHTTP processor — POST flowfiles to downstream HTTP endpoints
- [ ] GetHTTP source — poll an HTTP endpoint on a schedule
- [ ] PutFile processor — write to disk (replaces FileSink with proper directory/naming)
- [ ] GetFile source — watch a directory, ingest new files
- [ ] PutStdout processor — write to stdout (CLI pipelines, debugging)
- [ ] Connector lifecycle API (start/stop/status endpoints)

### Phase 2b: Observability — know what's happening
- [ ] Prometheus /metrics endpoint (processed count, queue depths, DLQ size, latency histograms)
- [ ] Structured JSON logging option (for log aggregation)
- [ ] FlowFile provenance — track processing history in attributes (processor chain, timestamps)
- [ ] Health checks with connector status
- [ ] /api/flow endpoint returns full DAG with queue depths (dashboard-ready)

### Phase 2c: Hardening — don't break in production
- [ ] Retry policies per processor (max retries, backoff strategy)
- [ ] Circuit breaker — stop routing after N consecutive failures, auto-recover
- [ ] Content store cleanup — periodic sweep of orphaned claims
- [ ] Queue persistence — optional WAL for crash recovery (bounded file, not a database)
- [ ] Config validation at startup (missing processors, broken routes, unknown types)

### Phase 2d: Developer experience
- [ ] `zinc-flow init` scaffolding — generate project with config, sample processors
- [ ] `zinc-flow validate` — check config without starting
- [ ] `zinc-flow replay` — CLI tool to replay DLQ entries
- [ ] Hot reload — watch config.yaml, reload flow graph on change
- [ ] Custom processor loading — register processors from external packages

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
- [ ] TUI dashboard (terminal UI with live queue depths + processor stats)
- [ ] Web UI — flow graph visualization, drag-and-drop processor wiring
- [ ] Provenance viewer — trace a FlowFile's path through the graph

---

## Phase 5 — Enterprise

- [ ] Role-based access control on management API
- [ ] Audit logging (who changed what, when)
- [ ] Multi-tenant flow isolation
- [ ] SLA monitoring and alerting
- [ ] Low-code web UI for flow authoring
