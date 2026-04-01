# Zinc Flow Roadmap

---

## Phase 1 — MVP (Local Dev)

Core runtime — single process, in-memory channels, everything works with `zinc run .`

- [x] FlowFile data class
- [x] ProcessorFn interface
- [x] LocalQueue (typed `Channel<FlowFile>`)
- [x] Basic processor worker loop (goroutine)
- [x] Filesystem sink (FileSink)
- [x] AddAttribute processor
- [ ] ProcessorResult sealed class (Single, Multiple, Routed, Drop)
- [ ] ProcessorWorker class (lifecycle: start/stop, stats, error handling)
- [ ] ProcessorGroup with start/stop/scale/swap
- [ ] Pipeline class with explicit wiring (`group.connect(from, to)`)
- [ ] ProcessorRegistry (register processor types at startup)
- [ ] Routing table indirection (workers read from shared route table, not direct channel refs)
- [ ] Back-pressure via bounded channels (natural from Go channel semantics)
- [ ] Dead letter queue channel for failed FlowFiles
- [ ] HTTP source (function-as-handler)
- [ ] CLI: `zinc run .` (already works) + terminal stats
- [ ] Terminal stats (msgs/sec, queue depth, errors per processor)

## Phase 2 — Production Ready

Multi-group, NATS JetStream between groups, REST API, live graph mutation.

### Core Infrastructure
- [ ] NATS JetStream for cross-group messaging (`nats-io/nats.go`)
- [ ] NatsQueue implementing FlowQueue interface
- [ ] Filesystem content store for large FlowFiles crossing groups
- [ ] State store (etcd or NATS KV) for flow graph, config, audit trail
- [ ] Prometheus `/metrics` endpoint

### Pipeline DSL
- [ ] Pipeline DSL with `->` chaining and group definitions
- [ ] Router with attribute-based predicates (RoutingRule, RoutingPredicate)

### Live Graph Mutation — Single-Pod (R4)
- [ ] `ProcessorGroup.addLive(name, registryKey)` — instantiate from registry, wire, start
- [ ] `ProcessorGroup.removeLive(name)` — stop, drain input channel, clean up routes
- [ ] `ProcessorGroup.reroute(from, output, newTarget)` — swap target channel under mutex
- [ ] `ProcessorGroup.splice(between, and, newName, registryKey)` — insert processor between two existing ones
- [ ] Mutex-protected routing table for atomic graph mutations
- [ ] Worker drain logic (forward remaining items to DLQ on removal)

### Live Graph Mutation — Cross-Pod (R4)
- [ ] GroupRouter with configurable publish subjects per output
- [ ] State store watcher — groups pick up subject changes dynamically, no restart
- [ ] Cross-pod reroute (change publish subject string, NATS delivers to new consumer)
- [ ] Cross-pod splice (deploy new pod, update subjects on both sides)
- [ ] JetStream buffering during transitions (no message loss)

### Management API (REST)
- [ ] `GET  /api/pipeline` — pipeline config
- [ ] `GET  /api/processors` — list processors with stats
- [ ] `POST /api/processors/start` — start processor
- [ ] `POST /api/processors/stop` — stop processor
- [ ] `POST /api/processors/scale` — scale replicas
- [ ] `POST /api/processors/swap` — hot-swap ProcessorFn
- [ ] `GET  /api/registry` — list available processor types
- [ ] `GET  /api/graph` — current graph topology
- [ ] `POST /api/graph/add` — add processor from registry
- [ ] `POST /api/graph/remove` — drain and remove processor
- [ ] `POST /api/graph/reroute` — redirect processor output
- [ ] `POST /api/graph/splice` — insert processor between two existing ones
- [ ] `GET  /api/queues` — queue depths
- [ ] `GET  /api/stats` — aggregate statistics
- [ ] `GET  /api/health` — health check

### CLI (Phase 2)
- [ ] `zinc flow status` — pipeline overview
- [ ] `zinc flow processors` — per-processor stats
- [ ] `zinc flow processor stop/start/scale <name>`
- [ ] `zinc flow queues` — channel/stream depths
- [ ] `zinc flow registry` — list available processor types
- [ ] `zinc flow graph` — show current topology
- [ ] `zinc flow graph add/remove/reroute/splice`

## Phase 3 — Cloud Native

K8s-native deployment, auto-scaling, observability.

- [ ] K8s operator: `zinc flow deploy` generates Deployments per group
- [ ] Auto-scaling groups based on NATS consumer lag
- [ ] Kafka queue backend option (pluggable via FlowQueue interface)
- [ ] OpenTelemetry tracing (FlowFile attributes carry trace context)
- [ ] TUI dashboard
- [ ] Wasm-based processor plugins (truly dynamic processors at runtime)
- [ ] Circuit breaker (stop routing to processor after N consecutive failures)

## Phase 4 — Enterprise

- [ ] Provenance tracking and lineage visualization
- [ ] Role-based access control on management API
- [ ] Audit logging
- [ ] Multi-pipeline management
- [ ] Low-code web UI
