# Zinc Flow Roadmap

---

## Phase 1 — MVP (Local Dev) ✓

Core runtime — single process, config-driven, everything works with `zinc run .`

- [x] FlowFile data class with attributes + Content sealed type (Raw, Records, Claim)
- [x] ProcessorFn interface + ProcessorResult sealed class (Single, Multiple, Routed, Dropped, Failure)
- [x] Avro-style GenericRecord with Schema, Field, FieldType
- [x] JSON RecordReader/RecordWriter
- [x] ContentStore interface + FileContentStore + MemoryContentStore
- [x] Content offload (>256KB → Claim reference in store)
- [x] NiFi FlowFile V3 binary serde (pack/unpack, single + multiple)
- [x] 5 built-in processors: AddAttribute, LogProcessor, FileSink, JsonToRecords, RecordsToJson
- [x] ProcessorRegistry with factory functions
- [x] IRS-style predicate routing engine (EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, composite AND/OR)
- [x] Fabric runtime — config-driven processor + route wiring, cycle detection
- [x] Dead letter queue (channel-backed, failure routing)
- [x] ServiceProvider (constructor-injected shared services, no DI container)
- [x] HTTP source (POST /ingest — raw body + V3 binary, /health endpoint)
- [x] HTTP delivery (POST to downstream endpoints, raw + V3)
- [x] Periodic terminal stats (processed/dropped/failures/dlq every 30s)
- [x] Test suite — 22 tests, 68 assertions (core, V3, processors, routing, fabric integration)

**Architecture:** 19 source files, 722 lines across `core/`, `fabric/`, `processors/`, `main.zn`

## Phase 2 — Production Ready

Multi-endpoint, persistent queues, REST API, live graph mutation.

### Core Infrastructure
- [ ] NATS JetStream for durable messaging between fabric instances
- [ ] Persistent content store (S3-compatible or filesystem with cleanup)
- [ ] State store (etcd or NATS KV) for flow graph, config, audit trail
- [ ] Prometheus `/metrics` endpoint
- [ ] Backpressure signaling between processors

### Routing & Processing
- [ ] Route priorities and ordering
- [ ] Processor chaining (explicit multi-hop without routing rules)
- [ ] Batch processing mode (process N FlowFiles at once)
- [ ] Schema registry for Avro schemas
- [ ] Schema-on-read inference in JsonToRecords (currently requires pre-defined schema)

### Live Graph Mutation
- [ ] Add/remove processors at runtime via API
- [ ] Enable/disable routing rules at runtime
- [ ] Hot-swap processor implementations
- [ ] Drain logic (forward remaining items to DLQ on removal)

### Management API (REST)
- [ ] `GET  /api/flow` — current flow graph (processors + routes)
- [ ] `GET  /api/processors` — list processors with stats
- [ ] `POST /api/processors` — add processor from registry
- [ ] `DELETE /api/processors/:name` — drain and remove
- [ ] `GET  /api/routes` — list routing rules
- [ ] `POST /api/routes` — add routing rule
- [ ] `PUT  /api/routes/:name/toggle` — enable/disable rule
- [ ] `GET  /api/registry` — list available processor types
- [ ] `GET  /api/stats` — aggregate statistics
- [ ] `GET  /api/dlq` — DLQ contents and count

## Phase 3 — Cloud Native

K8s-native deployment, auto-scaling, observability.

- [ ] K8s operator for fabric deployment
- [ ] Auto-scaling based on queue depth / consumer lag
- [ ] Kafka queue backend option (pluggable queue interface)
- [ ] OpenTelemetry tracing (FlowFile attributes carry trace context)
- [ ] Circuit breaker (stop routing after N consecutive failures)
- [ ] TUI dashboard

## Phase 4 — Enterprise

- [ ] Provenance tracking and lineage visualization
- [ ] Role-based access control on management API
- [ ] Audit logging
- [ ] Multi-flow management
- [ ] Low-code web UI
