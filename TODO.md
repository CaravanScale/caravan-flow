# Zinc Flow Roadmap

---

## Phase 1 — MVP (Local Dev)

- [ ] FlowFile data class
- [ ] ProcessorFn interface (constructor-injected, no annotations)
- [ ] LocalQueue (`Channel<T>`)
- [ ] ProcessorWorker (virtual thread-based worker loop)
- [ ] ProcessorGroup with start/stop/scale
- [ ] Pipeline with explicit wiring (no DSL yet)
- [ ] HTTP source
- [ ] Filesystem sink
- [ ] CLI: `zinc flow run pipeline.zn`
- [ ] Terminal stats (msgs/sec, queue depth, errors)

## Phase 2 — Production Ready

- [ ] Pipeline DSL with `->` chaining and group definitions
- [ ] NATS JetStream for cross-group messaging
- [ ] Filesystem content store for large FlowFiles crossing groups
- [ ] etcd/PostgreSQL state store (processor catalog, flow graph, audit trail)
- [ ] REST management API (start/stop/scale/swap/config)
- [ ] Processor catalog with hot-swap and rollback
- [ ] Prometheus `/metrics` endpoint (via Micrometer)

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
- [ ] Low-code web UI with Zinc expression validation
