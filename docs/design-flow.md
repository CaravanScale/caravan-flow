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

**NiFi's model. JVM's performance. Zinc's simplicity. Cloud-native from day one.**

A processor is a Zinc function. A pipeline connects processors with queues. Processors can be started, stopped, swapped, and scaled independently in production — without redeploying the whole pipeline.

Zinc transpiles to clean Java 25, giving us the full JVM ecosystem — virtual threads for parallelism, `Channel<T>` for bounded queues, GraalVM native-image for fast startup and small binaries, and access to every Java library on Maven Central.

## Core Requirements

### R1 — Processor Model

A processor is a Zinc function that takes a FlowFile and returns a ProcessorResult:

```zinc
ProcessorFn enrich_order = (flow) -> {
    var data = Json.parse(flow.content)
    data.put("enriched_at", Instant.now().toString())
    data.put("region", lookup_region(data.getString("zip_code")))
    return new Single(flow.withContent(Json.toBytes(data)))
}
```

- **Stateless by default** — no shared mutable state between invocations
- **Pure function** — input FlowFile in, output FlowFile(s) out
- **Swappable in production** — replace a processor without stopping the pipeline
- **Independent failure** — one processor crashing doesn't kill others
- **Independent scaling** — scale a slow processor to 10 instances while others stay at 1

### R2 — FlowFile

The unit of data flowing through the pipeline. Same concept as NiFi:

```zinc
data FlowFile(
    String id,                       // unique identifier
    Map<String, String> attributes,  // metadata (filename, mime.type, source, etc.)
    byte[] content,                  // the payload (1 byte to 100MB+)
    List<String> provenance = []     // processing history
)
```

- **Attributes** — small metadata map, copied freely between processors
- **Content** — the payload, potentially large (1-8MB typical, up to 100MB+)
- **Content by reference** — large content stored in content repository, FlowFile holds a reference (not copied between processors)
- **Provenance** — track where data came from, what happened to it

### R3 — Pipeline Definition

Pipelines connect processors with typed connections:

```zinc
pipeline order_processing
    // Sources
    source kafka("orders-topic", group="zinc-flow")

    // Processing chain
    -> validate_order
    -> enrich_order

    // Routing — fan out based on attributes or content
    -> route(
        status == "completed" -> process_payment,
        status == "pending"   -> hold_queue,
        _                     -> dead_letter
    )

    // Sinks
    process_payment -> sink kafka("payments-topic")
    hold_queue      -> sink s3("s3://bucket/pending/")
    dead_letter     -> sink filesystem("/var/zinc-flow/dead-letter/")
}
```

### R4 — Hot Swap / Live Processor Management

Critical requirement — must be able to in production:

- **Start** a processor (begin consuming from its input queue)
- **Stop** a processor (stop consuming, let queue buffer)
- **Swap** a processor (replace implementation, zero downtime)
- **Scale** a processor (add/remove instances)
- **Disable** a connection (stop routing to a branch)

```bash
zinc-flow processor stop enrich_order
zinc-flow processor swap enrich_order --version 2.1
zinc-flow processor start enrich_order
zinc-flow processor scale enrich_order --replicas 5
```

This implies:
- Each processor runs as an independent unit (not coupled to others)
- Processors communicate via queues (not function calls)
- Queue is the buffer — when a processor is stopped, messages accumulate
- Swap = stop old, deploy new, start new — queue bridges the gap

### R5 — Back-Pressure

When a downstream processor is slow or stopped, upstream should slow down:

- **Queue depth limits** — when queue reaches threshold, upstream blocks
- **Priority** — some FlowFiles are more important than others
- **Overflow** — when queue is full, optionally spill to disk or object storage

### R6 — Fault Tolerance

- **Processor crash** — auto-restart, reprocess from last checkpoint
- **At-least-once delivery** — FlowFile is not removed from input queue until processor confirms success
- **Dead letter queue** — failed FlowFiles routed to DLQ with error metadata
- **Circuit breaker** — if a processor fails N times, stop sending it traffic

```zinc
// Processor function — retries and dead letter are configured in ProcessorGroup wiring
ProcessorFn risky_transform = (flow) -> {
    var result = external_api_call(flow.content)
    return new Single(flow.withContent(result))
}

// Wiring: retries and dead letter configured at the group level
pipeline.addGroup(new ProcessorGroup("risky-transform", risky_transform)
    .withRetry(3, 1000))
```

### R7 — Sources and Sinks

Built-in connectors for common data sources/sinks:

| Source/Sink | Protocol |
|---|---|
| **Kafka** | Consume/produce topics |
| **S3** | Read/write objects |
| **Filesystem** | Watch directory, write files |
| **HTTP** | Receive webhooks, POST to endpoints |
| **Database** | JDBC query, CDC |
| **MQTT** | IoT message broker |

Custom sources/sinks are plain Zinc classes with constructor injection:

```zinc
class DirectoryWatcher {
    init String path
    init String pattern
    init FlowQueue outputQueue

    init(String path, String pattern, FlowQueue outputQueue) {
        this.path = path
        this.pattern = pattern
        this.outputQueue = outputQueue
    }

    pub fn start() {
        for file in Files.list(Path.of(path)) {
            if file.toString().matches(pattern) {
                var ff = new FlowFile(
                    UUID.randomUUID().toString(),
                    {"filename": file.getFileName().toString(), "path": file.toString()},
                    Files.readAllBytes(file),
                    System.currentTimeMillis(),
                    []
                )
                outputQueue.put(ff)
            }
        }
    }
}
```

### R8 — GUI / Management Interface

Operators need to:

- **Visualize** the pipeline graph (processors, connections, queue depths)
- **Monitor** throughput, latency, error rates per processor
- **Control** start/stop/scale/swap processors
- **Inspect** FlowFiles (view attributes, content preview, provenance)
- **Configure** processor parameters without redeployment

Options:
- Web UI (React/Vue) talking to a REST API
- Terminal UI (TUI) for CLI-first environments
- REST API is the foundation — any UI is just a client

### R9 — Cloud Native

- **K8s-native** — each processor group is a pod (or a process in a pod)
- **Horizontal scaling** — scale processors independently via replicas
- **Stateless processors** — no local state dependency (state in external stores)
- **Config as code** — pipeline definitions are `.zn` files in git
- **No single point of failure** — no "NiFi cluster coordinator"

### R10 — Lightweight

- **No NiFi bloat** — Zinc compiles to lean JVM bytecode or GraalVM native-image
- **Fast startup** — native-image starts in <100ms (not 30s like NiFi)
- **Embeddable** — can run a mini pipeline inside an existing JVM app
- **Edge-capable** — native-image runs on a Raspberry Pi, a Lambda function, or a K8s pod
- **Small footprint** — no 1GB heap requirement; minimal memory for small pipelines

---

## Architecture Options

### Decision: Processor Groups (evolved from Option C)

Neither NiFi's monolith nor DeltaFi's container-per-processor. The developer chooses group boundaries.

- A **Processor Group** = unit of deployment (one pod, one process)
- Within a group: virtual threads + `Channel<T>` (in-memory, bounded queues, high throughput)
- Between groups: NATS JetStream (serialization only at boundaries)
- Local dev: all groups collapse into one process
- K8s: each group becomes a Deployment with its own replica count

```
Pod 1 (ingest-group, 1 replica):
  [http-source] -> [parse] -> [validate]     ← virtual threads, Channel<T>
                                    |
                             NATS JetStream   ← cross-group boundary
                                    |
Pod 2 (enrich-group, 10 replicas):            ← slow, scaled out
  [enrich] -> [lookup]                        ← virtual threads, Channel<T>
                  |
             NATS JetStream
                  |
Pod 3 (output-group, 2 replicas):
  [format] -> [kafka-sink]                    ← virtual threads, Channel<T>
```

```bash
zinc flow run pipeline.zn                                    # local, all in one process
zinc flow run pipeline.zn --mode distributed --nats nats://localhost:4222  # distributed
zinc flow deploy pipeline.zn --namespace prod --nats nats://nats:4222     # K8s
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
| **Luigi** | Simple task dependencies | No streaming, no real-time |
| **Temporal** | Workflow durability, replay | Too general, not data-flow specific |
| **Camel** | EIP vocabulary (Splitter, Aggregator, ContentBasedRouter), uniform connector model (Component → Endpoint → Consumer/Producer), nuanced error handling (per-exception routing, redelivery policies) | Runtime expression languages (Simple/SpEL), XML/YAML DSL, library-mode coupling (no independent scaling) |

---

## Content Repository — Large Payload Strategy

NiFi's key insight: FlowFile content is stored in a content repository, and processors pass references (claims) not copies.

For Zinc Flow:

```
FlowFile passed between processors:
  { attributes: {...}, content_ref: "content://abc123" }   ← 200 bytes

Actual content stored separately:
  content://abc123 → 4MB payload in content store
```

Content store options by mode:
- **Local dev**: filesystem directory (e.g., `/tmp/zinc-flow/content/`)
- **Production**: S3, MinIO, or shared filesystem
- **In-memory**: for small payloads (<64KB), skip the store entirely

This means passing a 4MB FlowFile between processors costs ~200 bytes (the reference), not 4MB.

---

## Research Findings (2026-03-18, updated 2026-03-23)

### Architecture Decision: Zinc on JVM with Virtual Threads

Zinc transpiles to Java 25. This gives Zinc Flow access to the full JVM ecosystem and proven production runtime:

- **Virtual threads (Project Loom)**: millions of concurrent threads with near-zero overhead — purpose-built for the worker-loop pattern where each processor consumes from a queue
- **Channel\<T\> (ArrayBlockingQueue)**: bounded blocking queues built into Zinc — `put()` blocks when full (natural back-pressure), `take()` blocks when empty
- **GraalVM native-image**: compiles to a standalone binary with ~100ms startup, no JVM needed at runtime — matches the "lightweight, fast startup" requirement
- **JVM JIT compilation**: long-running processor loops get faster over time as HotSpot optimizes hot paths
- **Proven at scale**: NiFi itself runs on the JVM and handles 10K-100K msgs/sec — Zinc Flow targets the same runtime with less bloat

NiFi's problem was never the JVM — it was the architectural bloat. Zinc Flow keeps the JVM's strengths (concurrency, ecosystem, reliability) while shedding NiFi's monolithic design.

### Key Insight: Zinc Gives Us Java Without the Ceremony

The ergonomic gap between NiFi processors and Zinc Flow processors:

```
NiFi processor:     Java class + @Tags + @CapabilityDescription + PropertyDescriptor[]
                    + AbstractProcessor + onTrigger() + ProcessSession + FlowFile API
                    + Maven module + NAR packaging → 100+ lines of boilerplate

Zinc Flow processor: ProcessorFn name = (ff) -> { ... } → just the logic
```

Zinc's `data` classes replace Java record boilerplate. `ProcessorFn` interface replaces NiFi's `AbstractProcessor` subclassing. The transpiler handles the ceremony — the developer writes the logic.

### Stack

```
Zinc Flow (JVM)         — orchestration, queues, routing, lifecycle
Virtual Threads         — real parallelism between processors (no GIL, no thread pool sizing)
Channel<T>              — bounded queues with natural back-pressure
GraalVM native-image    — single binary deployment, fast startup, small footprint
Java ecosystem          — Kafka, NATS, JDBC, Jackson, Netty — all available via Mill/Maven
```

## Open Questions — Resolved

All resolved in `design-flow-runtime.md`. Summary:

1. **Queue technology** — NATS JetStream for cross-group messaging. etcd/PostgreSQL for state. Filesystem for large content. Pluggable interface. Never Rook/Ceph.
2. **GUI framework** — REST API first, TUI second, web UI later once validated.
3. **Processor discovery** — Static imports for dev. Processor catalog (in state store) for prod with hot-swap via classloader (JVM mode) or queue-bridged rolling restart (native-image/K8s).
4. **Versioning** — Processor catalog with name@version. Every flow change creates a revision with audit trail. Instant rollback.
5. **Schema enforcement** — No. Content is opaque `byte[]`. Validation is processor logic, up to the dataflow developer.
6. **Multi-tenancy** — Namespace isolation per pipeline on shared infrastructure.
7. **Routing language** — Two tiers: attribute-based predicates (IRS-inspired, GraalVM-safe, covers 80%) + compiled `ProcessorFn` implementations for complex logic. No runtime eval. Low-code UI maps directly to predicates.
8. **Monitoring** — Terminal stats Phase 1, Prometheus `/metrics` Phase 2, OpenTelemetry tracing Phase 3.
9. **State management** — External state stores (etcd, PostgreSQL). Processors are stateless from the runtime's perspective.
10. **Ordering guarantees** — Best-effort FIFO. FlowFiles are independent units, ordering is a non-issue for typical use cases.

---

## Implementation Phases

See `design-flow-runtime.md` for detailed phase breakdown.

### Phase 1 — MVP (Local Dev)
- FlowFile data class, ProcessorFn interface
- LocalQueue (`Channel<T>`), ProcessorWorker (virtual thread-based)
- ProcessorGroup with start/stop/scale
- Pipeline with explicit wiring (no DSL yet)
- HTTP source, filesystem sink
- CLI: `zinc flow run pipeline.zn`
- Terminal stats (msgs/sec, queue depth, errors)

### Phase 2 — Production Ready
- Pipeline DSL with `->` chaining and group definitions
- NATS JetStream for cross-group messaging
- Filesystem content store for large FlowFiles crossing groups
- etcd/PostgreSQL state store (processor catalog, flow graph, audit trail)
- REST management API (start/stop/scale/swap/config)
- Processor catalog with hot-swap and rollback
- Prometheus `/metrics` endpoint (via Micrometer)

### Phase 3 — Cloud Native
- K8s operator: `zinc flow deploy` generates Deployments per group
- Auto-scaling groups based on NATS consumer lag
- Kafka queue backend option (pluggable)
- OpenTelemetry tracing
- TUI dashboard

### Phase 4 — Enterprise
- Provenance tracking and lineage visualization
- Role-based access control on management API
- Audit logging
- Multi-pipeline management
- Low-code web UI with Zinc expression validation
