# Zinc Flow Roadmap

---

## Phase 1 — MVP (Local Dev) ✓

Core runtime — single process, config-driven, everything works with `zinc run .`

- [x] FlowFile data class with attributes + Content sealed type (Raw, Records, Claim)
- [x] ProcessorFn interface + ProcessorResult sealed class (Single, Multiple, Routed, Dropped, Failure)
- [x] Avro-style GenericRecord with Schema, Field, FieldType
- [x] JSON RecordReader/RecordWriter + schema-on-read inference
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
- [x] Management API — read-only (flow, processors, routes, registry, stats, dlq)
- [x] Management API — mutations (add/remove processors, add/remove/toggle routes)
- [x] Test suite — 22 tests, 68 assertions

## Phase 2 — Production Ready

Connector framework, NATS messaging, backpressure, processor lifecycle.

**Architecture:** Core engine stays self-contained (zero deps). External messaging
is handled by connector processors (PutNats, GetNats, etc.) — just regular
ProcessorFn implementations. Within a group: synchronous in-process routing.
Between groups: connector processors publish/consume via NATS/Kafka/SQS.

### Phase 2a: Connector framework + NATS (zinc-flow)
- [ ] ConnectorSource interface (start/stop/isRunning lifecycle)
- [ ] Refactor HttpSource to implement ConnectorSource
- [ ] PutNats processor (serialize V3, publish to subject)
- [ ] GetNats source connector (subscribe, unpack V3, feed Fabric)
- [ ] Connector lifecycle API (start/stop endpoints)
- [ ] Bounded ingress queue (backpressure within group)
- [ ] Add nats.go dependency
- [ ] Integration tests with embedded NATS
- [ ] Example: two zinc-flow instances connected via NATS

### Phase 2b: K8s operator (zinc-flow-operator, separate repo)
- [ ] Scaffold with kubebuilder
- [ ] ProcessorGroup CRD + controller (deploy zinc-flow pods from CRD spec)
- [ ] Config generation (CRD spec → config.yaml ConfigMap)
- [ ] Flow CRD + controller (global topology, cross-group wiring)
- [ ] Connection validation (PutNats subject matches GetNats subject)
- [ ] Status aggregation across groups
- [ ] E2e test with kind cluster

### Phase 2c: Production hardening
- [ ] NATS auth (credentials, TLS)
- [ ] Retry policies per connector
- [ ] Graceful shutdown (drain connectors, flush DLQ)
- [ ] Health checks including connector status
- [ ] Prometheus /metrics endpoint

## Phase 3 — Ecosystem

Additional connectors, observability, developer experience.

- [ ] PutKafka / GetKafka connectors
- [ ] PutSQS / GetSQS connectors
- [ ] PutHTTP / GetHTTP formalized connectors
- [ ] OpenTelemetry tracing (FlowFile attributes carry trace context)
- [ ] Circuit breaker (stop routing after N consecutive failures)
- [ ] Auto-scaling based on consumer lag (operator)
- [ ] TUI dashboard
- [ ] Schema registry for Avro schemas

## Phase 4 — Enterprise

- [ ] Provenance tracking and lineage visualization
- [ ] Role-based access control on management API
- [ ] Audit logging
- [ ] Multi-flow management
- [ ] Low-code web UI
