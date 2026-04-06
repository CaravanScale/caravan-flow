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
- [x] HTTP source (POST /ingest — raw body + V3 binary, /health endpoint)
- [x] HTTP delivery (POST to downstream endpoints, raw + V3)
- [x] Periodic terminal stats (processed/dropped/failures/dlq every 30s)
- [x] Management API — read-only (flow, processors, routes, registry, stats, dlq)
- [x] Management API — mutations (add/remove processors, add/remove/toggle routes)

## Phase 1.5 — Flow Engine (Transactional Delivery) ✓

Reliability model — at-least-once delivery, backpressure, lifecycle management.

- [x] Provider interface with lifecycle (ComponentState: ENABLED/DRAINING/DISABLED)
- [x] Three concrete providers: ConfigProvider, LoggingProvider, ContentProvider
- [x] ProcessorContext — global provider bag with dependency tracking
- [x] ScopedContext — per-processor provider isolation via `requires`
- [x] ProcessorFactory signature: `Fn<(ScopedContext, Map), ProcessorFn>`
- [x] FlowQueue — transactional queue with claim/ack/nack and visibility timeout
- [x] ProcessSession — transaction boundary (claim → process → route → ack)
- [x] IRS all-or-nothing fan-out with capacity pre-check
- [x] Backpressure — bounded queues (count + bytes), nack propagation, HTTP 503
- [x] Async processing — per-processor goroutine loops + ingest router loop
- [x] DLQ — inspectable, replayable to source queue via API
- [x] Feature flags — enable/disable processors at runtime via API
- [x] Dependency cascade — disable provider → drain + disable dependent processors
- [x] Enable check — refuse to enable processor if required provider is disabled
- [x] Config: `enabled` flag, `requires` list, `defaults.backpressure.*`, per-processor overrides
- [x] Provider API: list, enable, disable with cascade
- [x] Queue stats API: visible/invisible/total per processor
- [x] DLQ API: list, replay, replay-all, delete
- [x] Test suite — 29 tests, 128+ assertions, 9 end-to-end scenarios

### Remaining (Phase 1.5)
- [ ] Map-keyed YAML config (currently list-indexed — depends on Zinc config parser)
- [ ] Visibility timeout e2e test (requires sleep-based async test)
- [ ] Graceful shutdown (shutdownAll on SIGTERM)

## Phase 2 — Production Ready

Connector framework, NATS messaging, processor lifecycle.

### Phase 2a: Connector framework + NATS (zinc-flow)
- [ ] ConnectorSource interface (start/stop/isRunning lifecycle)
- [ ] Refactor HttpSource to implement ConnectorSource
- [ ] PutNats processor (serialize V3, publish to subject)
- [ ] GetNats source connector (subscribe, unpack V3, feed Fabric)
- [ ] Connector lifecycle API (start/stop endpoints)
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
