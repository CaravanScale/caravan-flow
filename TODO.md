# Zinc Flow Roadmap

---

## Phase 1 — MVP (Local Dev)

- [x] FlowFile data class
- [x] ProcessorFn interface
- [x] LocalQueue (typed `Channel<FlowFile>`)
- [x] Basic processor worker loop (goroutine)
- [x] Filesystem sink (FileSink)
- [x] AddAttribute processor
- [ ] ProcessorWorker class (lifecycle: start/stop, stats, error handling)
- [ ] ProcessorGroup with start/stop/scale
- [ ] Pipeline class with explicit wiring
- [ ] ProcessorResult sealed class (Single, Multiple, Routed, Drop)
- [ ] HTTP source (function-as-handler — unblocked, see httpserver.zn)
- [ ] Back-pressure via bounded channels
- [ ] CLI: `zinc run .` (already works) + terminal stats
- [ ] Terminal stats (msgs/sec, queue depth, errors)

## Phase 2 — Production Ready

- [ ] Pipeline DSL with `->` chaining and group definitions
- [ ] NATS JetStream for cross-group messaging (Go client: `nats-io/nats.go`)
- [ ] Filesystem content store for large FlowFiles crossing groups
- [ ] State store (etcd or PostgreSQL) for flow graph, config, audit trail
- [ ] REST management API (start/stop/scale/swap/config) via `net/http`
- [ ] Processor catalog with hot-swap and rollback
- [ ] Prometheus `/metrics` endpoint
- [ ] Router with attribute-based predicates

## Phase 3 — Cloud Native

- [ ] K8s operator: `zinc flow deploy` generates Deployments per group
- [ ] Auto-scaling groups based on NATS consumer lag
- [ ] Kafka queue backend option (pluggable)
- [ ] OpenTelemetry tracing
- [ ] TUI dashboard

## Phase 4 — Enterprise

- [ ] Provenance tracking and lineage visualization
- [ ] Role-based access control on management API
- [ ] Audit logging
- [ ] Multi-pipeline management
- [ ] Low-code web UI
