# Design: Zinc Flow — Lightweight NiFi-Inspired Flow Processing

> **Status**: REQUIREMENTS COMPLETE — see `design-flow-runtime.md` for architecture and implementation design

## The Problem

NiFi is the gold standard for data flow processing but has real problems:

| Problem | Impact |
|---|---|
| **Bloated** | 1GB+ JVM heap, hundreds of bundled processors, massive install |
| **Not cloud-native** | Designed for single-node; cluster mode is bolted on, not elastic |
| **Not horizontally scalable** | Can't auto-scale individual processors independently |
| **Heavy** | Can't run on edge, can't embed in existing apps |
| **Java-only processors** | Writing custom processors requires Java, Maven, NAR packaging |

**MiNiFi** tried to solve "lightweight" but is neglected and missing features.

**DeltaFi** tried to solve "cloud-native" but:
- Too many tools/technologies in the stack
- Docker containers as processor boundary (slow startup, resource overhead)
- Unproven at scale
- Complex to operate

## What We Want

**NiFi's model. Go's performance. Zinc's simplicity. Cloud-native from day one.**

A processor is a Zinc class. A pipeline connects processors with typed channels. Processors can be started, stopped, swapped, and scaled independently in production — without redeploying the whole pipeline.

Zinc transpiles to clean Go, giving us goroutines for concurrency, typed channels for bounded queues, and native binaries (~2.5MB) with instant startup.

## Core Requirements

### R1 — Processor Model

A processor is a Zinc class implementing `ProcessorFn`:

```zinc
class EnrichOrder : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        var enriched = ff.content + "\n[enriched_at=" + str(time.Now().Unix()) + "]"
        return Single(FlowFile(ff.id, ff.attributes, enriched, ff.timestamp))
    }
}
```

- **Stateless by default** — no shared mutable state between invocations
- **Pure function** — input FlowFile in, output FlowFile(s) out
- **Swappable in production** — replace a processor without stopping the pipeline
- **Independent failure** — one processor crashing doesn't kill others
- **Independent scaling** — scale a slow processor to 10 goroutines while others stay at 1

### R2 — FlowFile

The unit of data flowing through the pipeline:

```zinc
data FlowFile(
    String id,
    Map<String, String> attributes,
    String content,
    int timestamp
)
```

- **Attributes** — small metadata map, copied freely between processors
- **Content** — the payload
- **Provenance** — track where data came from, what happened to it (Phase 2)

### R3 — Pipeline Definition

Pipelines connect processors with typed channels:

```zinc
// Phase 1: explicit wiring
var pipeline = Pipeline("order_processing")
pipeline.addProcessor("validate", ValidateOrder())
pipeline.addProcessor("enrich", EnrichOrder())
pipeline.addProcessor("sink", FileSink("/tmp/output"))
pipeline.connect("validate", "enrich")
pipeline.connect("enrich", "sink")
pipeline.run()
```

```zinc
// Phase 2: DSL with -> chaining
pipeline order_processing {
    source http(port = 8080)
    -> validate_order
    -> enrich_order
    -> route(
        status == "completed" -> process_payment,
        status == "pending"   -> hold_queue,
        _                     -> dead_letter
    )
    process_payment -> sink file("/var/zinc-flow/output/")
}
```

### R4 — Hot Swap / Live Processor Management

Critical requirement — must be able to in production:

- **Start** a processor (begin consuming from its input channel)
- **Stop** a processor (stop consuming, let channel buffer)
- **Swap** a processor (replace implementation, zero downtime)
- **Scale** a processor (add/remove goroutines)
- **Add** a new processor to the running graph
- **Remove** a processor from the graph (drain, then disconnect)
- **Reroute** an output from one processor to a different downstream processor

This implies:
- Each processor runs as an independent goroutine (not coupled to others)
- Processors communicate via channels (not function calls)
- Channel is the buffer — when a processor is stopped, messages accumulate
- Swap = stop old goroutine, start new goroutine with new ProcessorFn — channel bridges the gap

#### Live Graph Mutation (Single-Pod)

In the single-pod case, all processors share one process. You can't deploy new code — the binary is fixed. So "add a processor" means **instantiate from a registry**, not load new code. This is the NiFi model: processors are pre-installed, the graph is configuration.

**Processor Registry**: All available `ProcessorFn` implementations register at startup. Adding a processor at runtime picks from the registry by name and wires it into the graph.

**Channel Indirection**: Processors don't hold direct channel references. Instead, the `ProcessorGroup` owns a routing table mapping `(processor, output-name) → channel`. The worker loop reads from this table on each send. Rerouting = swap the channel pointer in the table under a mutex. Upstream never pauses — it sees the new target on its next `send()`.

**Graph mutation operations** (all atomic under a single mutex):

| Operation | Steps |
|-----------|-------|
| **Add processor** | Create worker from registry → create input channel → update routing table → start worker |
| **Remove processor** | Stop worker → drain input channel (forward to DLQ or next) → remove from routing table |
| **Reroute** | Lock routing table → swap target channel → unlock. Zero downtime — upstream goroutines see new target on next send |
| **Splice** (insert between two processors) | Create new worker → point A's output to new worker's input → point new worker's output to B's input → start new worker |

**Why this works in-process**: Go channels are goroutine-safe. A channel reference can be swapped atomically (pointer-sized write). The mutex protects the routing table, not the channels themselves. Upstream goroutines that are blocked on `send()` will complete into whichever channel was current when they entered the send — this is safe because the old channel is drained before being discarded.

**Constraint**: only processors compiled into the binary are available. For truly dynamic processors (user-defined at runtime), Phase 3 adds Wasm-based processor plugins — but that's a separate concern from graph mutation.

### R5 — Back-Pressure

When a downstream processor is slow or stopped, upstream should slow down:

- **Channel capacity limits** — when channel is full, upstream blocks on `send()`
- **Bounded channels** — Go channels have natural back-pressure when buffered

### R6 — Fault Tolerance

- **Processor crash** — goroutine recovers via defer/recover, reprocess from last item
- **Dead letter queue** — failed FlowFiles routed to DLQ channel with error metadata
- **Circuit breaker** — if a processor fails N times, stop sending it traffic

### R7 — Sources and Sinks

Built-in connectors for common data sources/sinks:

| Source/Sink | Protocol |
|---|---|
| **HTTP** | Receive webhooks, POST to endpoints |
| **Filesystem** | Watch directory, write files |
| **Kafka** | Consume/produce topics (via Go client) |
| **NATS** | Pub/sub and JetStream |

Custom sources/sinks are plain Zinc classes.

### R8 — GUI / Management Interface

- REST API is the foundation — any UI is just a client
- Terminal stats for CLI-first environments (Phase 1)
- TUI dashboard (Phase 3)
- Web UI if there's demand (Phase 4)

### R9 — Cloud Native

- **K8s-native** — each processor group is a pod
- **Horizontal scaling** — scale processors independently via replicas
- **Stateless processors** — no local state dependency
- **Config as code** — pipeline definitions are `.zn` files in git

### R10 — Lightweight

- **Native binary** — `zinc build .` produces ~2.5MB binary
- **Instant startup** — Go binary starts in milliseconds
- **Embeddable** — can run a mini pipeline inside an existing Go app
- **Edge-capable** — runs on a Raspberry Pi, a Lambda function, or a K8s pod
- **Small footprint** — minimal memory for small pipelines

---

## Architecture: Processor Groups

Neither NiFi's monolith nor DeltaFi's container-per-processor. The developer chooses group boundaries.

- A **Processor Group** = unit of deployment (one pod, one process)
- Within a group: **goroutines + typed channels** (zero serialization, reference passing, high throughput)
- Between groups: **NATS JetStream** (serialization only at boundaries)
- Local dev: all groups collapse into one process
- K8s: each group becomes a Deployment with its own replica count

```
Pod 1 (ingest-group, 1 replica):
  [http-source] -> [parse] -> [validate]     ← goroutines, Channel<FlowFile>
                                    |
                             NATS JetStream   ← cross-group boundary
                                    |
Pod 2 (enrich-group, 10 replicas):            ← slow, scaled out
  [enrich] -> [lookup]                        ← goroutines, Channel<FlowFile>
                  |
             NATS JetStream
                  |
Pod 3 (output-group, 2 replicas):
  [format] -> [file-sink]                     ← goroutines, Channel<FlowFile>
```

```bash
zinc run .                                              # local, all in one process
zinc run . --mode distributed --nats nats://localhost:4222  # distributed
```

See `design-flow-runtime.md` for full architecture details.

---

## Prior Art — What to Learn From

| System | What to steal | What to avoid |
|---|---|---|
| **NiFi** | FlowFile model, provenance, back-pressure, processor lifecycle | JVM bloat, monolithic cluster, NAR packaging |
| **DeltaFi** | K8s-native, plugin architecture | Docker-per-processor overhead, complexity |
| **MiNiFi** | Lightweight footprint | Neglected, missing features |
| **Prefect** | Decorator-based task definition | Batch-oriented, not streaming |
| **Flink** | Exactly-once, checkpointing, watermarks | Operational complexity |
| **Camel** | EIP vocabulary, uniform connector model | Runtime expression languages, XML/YAML DSL |

---

## Stack

```
Zinc Flow             — orchestration, queues, routing, lifecycle
Go goroutines         — real parallelism (no GIL, M:N scheduling)
Typed channels        — bounded queues with natural back-pressure
Native binary         — single binary deployment, instant startup, ~2.5MB
Go ecosystem          — net/http, encoding/json, nats.go — all available
```

---

## Implementation Phases

See `design-flow-runtime.md` for detailed phase breakdown.

### Phase 1 — MVP (Local Dev)
- FlowFile data class, ProcessorFn interface, ProcessorResult sealed class
- Typed channels as LocalQueue, ProcessorWorker (goroutine-based)
- ProcessorGroup with start/stop/scale
- Pipeline with explicit wiring (no DSL yet)
- HTTP source, filesystem sink
- Terminal stats (msgs/sec, queue depth, errors)

### Phase 2 — Production Ready
- Pipeline DSL with `->` chaining and group definitions
- NATS JetStream for cross-group messaging
- Filesystem content store for large FlowFiles crossing groups
- State store (etcd/PostgreSQL) for flow graph, config, audit trail
- REST management API
- Prometheus `/metrics` endpoint

### Phase 3 — Cloud Native
- K8s operator: generates Deployments per group
- Auto-scaling groups based on NATS consumer lag
- Kafka queue backend option (pluggable)
- OpenTelemetry tracing
- TUI dashboard

### Phase 4 — Enterprise
- Provenance tracking and lineage visualization
- Role-based access control on management API
- Audit logging
- Multi-pipeline management
