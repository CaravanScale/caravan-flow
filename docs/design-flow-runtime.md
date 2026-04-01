# Design: Zinc Flow Runtime — Architecture & Implementation

> **Status**: DESIGN — architecture validated, ready for Phase 1 implementation

## Design Principles

### Program to Interfaces, Not Implementations

Every major component in Zinc Flow is defined as an interface. Implementations are pluggable.

Core interfaces:

| Interface | Purpose | Implementations |
|-----------|---------|----------------|
| `FlowQueue` | Message passing between processors | `LocalQueue` (channel), `NatsQueue`, `KafkaQueue` |
| `ContentStore` | Large FlowFile content storage | `FileContentStore`, `S3ContentStore` |
| `StateStore` | Flow graph, catalog, audit trail | `EtcdStateStore`, `PostgresStateStore`, `LocalStateStore` |
| `SecretsProvider` | Credential resolution | `EnvSecrets`, `FileSecrets` |
| `MetricsExporter` | Observability output | `ConsoleMetrics`, `PrometheusMetrics` |

### Dogfood Zinc — Never Fall Back to Raw Go

Zinc Flow is written in Zinc. When we hit something awkward or missing — **that's a signal to improve Zinc, not to drop into raw Go.**

Each gap found is a language improvement tracked on the Zinc repo. The runtime is the proving ground.

---

## Architecture Decision

**Processor Groups** — the middle ground between NiFi's monolith and DeltaFi's container-per-processor.

- A **Processor Group** is the unit of deployment (one pod, one process)
- Within a group: **goroutines + typed channels** (zero serialization, reference passing)
- Between groups: **NATS JetStream** (serialization only at group boundaries)
- **Local dev**: all groups collapse into one process, everything is in-memory channels
- **K8s**: each group becomes a Deployment with its own replica count

### Why Go

| Feature | Go Advantage |
|---------|-------------|
| **Goroutines** | Millions of concurrent lightweight threads, M:N scheduling, purpose-built for concurrent I/O and queue consumption loops |
| **Typed channels** | Bounded blocking queues — `send()` blocks when full (back-pressure), `recv()` blocks when empty. Zero-copy reference passing within a process |
| **Native binary** | Single binary, ~2.5MB, instant startup. No runtime dependencies |
| **Cross-compilation** | `GOOS=linux GOARCH=arm64` — build for any target from any host |

---

## Core Types

### FlowFile

```zinc
data FlowFile(
    String id,
    Map<String, String> attributes,
    String content,
    int timestamp
)
```

FlowFiles are passed by reference through channels within a process — no serialization, no copy.

### ProcessorResult

Sealed class hierarchy — the worker loop matches exhaustively:

```zinc
sealed class ProcessorResult {
    data Single(FlowFile ff)
    data Multiple(List<FlowFile> ffs)
    data Routed(String route, FlowFile ff)
    data object Drop
}
```

Processors return `ProcessorResult` explicitly:
- `Single(ff)` — 1:1 transform
- `Multiple(list)` — 1:N split/fan-out
- `Routed("name", ff)` — route to a named output
- `Drop()` — filter/discard

```zinc
// 1:1 transform
class Enrich : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        return Single(ff.withAttribute("enriched", "true"))
    }
}

// 1:N split
class SplitLines : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        var lines = ff.content.split("\n")
        var files = lines.map(line -> FlowFile(uuid(), {}, line, now()))
        return Multiple(files)
    }
}

// Routing
class ValidateOrder : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        if ff.attributes.get("order_id") == "" {
            return Routed("invalid", ff)
        }
        return Routed("valid", ff)
    }
}

// Filter
class FilterEmpty : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        if len(ff.content) == 0 { return Drop() }
        return Single(ff)
    }
}
```

---

## Processor Model

### ProcessorWorker — Goroutine-Based Worker Loop

Each processor runs as one or more goroutines consuming from an input channel:

```zinc
class ProcessorWorker {
    String name
    var ProcessorFn fn
    var input = Channel<FlowFile>(10000)
    var outputs = Map<String, Channel<FlowFile>>{}
    var running = false
    var replicas = 1
    var stats = ProcessorStats()

    init(String name, ProcessorFn fn) {
        this.name = name
        this.fn = fn
    }

    pub fn start() {
        running = true
        for i in 0..replicas {
            spawn { runLoop() }
        }
    }

    pub fn stop() {
        running = false
    }

    pub fn scale(int n) {
        replicas = n
    }

    fn runLoop() {
        while running {
            var ff = input.recv()
            var start = time.Now()

            var result = fn.process(ff)
            // error handling: recover from panic, route to DLQ

            var elapsed = time.Since(start)
            stats.record(elapsed)
            routeOutput(result)
        }
    }

    fn routeOutput(ProcessorResult result) {
        match result {
            case Single(ff) {
                outputs.get("default").send(ff)
            }
            case Multiple(ffs) {
                for ff in ffs {
                    outputs.get("default").send(ff)
                }
            }
            case Routed(route, ff) {
                outputs.getOrDefault(route, outputs.get("default")).send(ff)
            }
            case Drop {
                // discard
            }
        }
    }
}
```

### ProcessorGroup

```zinc
class ProcessorGroup {
    String name
    var workers = Map<String, ProcessorWorker>{}

    pub fn addProcessor(String name, ProcessorFn fn) {
        workers.put(name, ProcessorWorker(name, fn))
    }

    pub fn connect(String from, String to) {
        var source = workers.get(from)
        var target = workers.get(to)
        source.outputs.put("default", target.input)
    }

    pub fn start() {
        for (_, worker) in workers {
            worker.start()
        }
    }

    pub fn stop() {
        for (_, worker) in workers {
            worker.stop()
        }
    }

    pub fn stopProcessor(String name) {
        workers.get(name).stop()
    }

    pub fn startProcessor(String name) {
        workers.get(name).start()
    }

    pub fn scaleProcessor(String name, int replicas) {
        workers.get(name).scale(replicas)
    }

    pub fn swapProcessor(String name, ProcessorFn newFn) {
        var worker = workers.get(name)
        worker.stop()
        worker.fn = newFn
        worker.start()
    }
}
```

---

## Pipeline

```zinc
class Pipeline {
    String name
    var groups = Map<String, ProcessorGroup>{}
    var mode = "local"

    init(String name) {
        this.name = name
    }

    pub fn addGroup(ProcessorGroup group) {
        groups.put(group.name, group)
    }

    pub fn run() {
        print("Pipeline '{name}' starting ({mode} mode)")
        for (_, group) in groups {
            group.start()
        }
        waitForShutdown()
    }

    fn waitForShutdown() {
        // Block on signal (SIGINT/SIGTERM)
        var sig = Channel(1)
        // signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
        sig.recv()
        print("Shutting down...")
        for (_, group) in groups {
            group.stop()
        }
    }

    pub fn status(): Map<String, String> {
        var result = Map<String, String>{}
        for (groupName, group) in groups {
            for (procName, worker) in group.workers {
                result.put("{groupName}/{procName}", worker.stats.summary())
            }
        }
        return result
    }
}
```

---

## Queue Abstraction

```zinc
interface FlowQueue {
    fn put(FlowFile ff)
    fn poll(int timeoutMs): FlowFile?
    fn size(): int
}

// In-memory — typed channel (within a group)
class LocalQueue : FlowQueue {
    var ch = Channel<FlowFile>(10000)

    pub fn put(FlowFile ff) {
        ch.send(ff)  // blocks when full → natural back-pressure
    }

    pub fn poll(int timeoutMs): FlowFile? {
        // select with timeout
        return ch.recv()
    }

    pub fn size(): int {
        return len(ch)
    }
}

// NATS JetStream (between groups) — Phase 2
class NatsQueue : FlowQueue {
    // Go NATS client: nats-io/nats.go
    // Serialize FlowFile to JSON at group boundary
    // Consumer groups for competing consumers (scaling)
}
```

---

## Sources and Sinks

Sources push FlowFiles into the pipeline. Sinks consume them. Both are goroutines.

```zinc
// HTTP source — receives requests, creates FlowFiles
class HttpSource {
    int port
    var output = Channel<FlowFile>(1024)

    init(int port) {
        this.port = port
    }

    pub fn start() {
        http.HandleFunc("/ingest", handleIngest)
        spawn { http.ListenAndServe(":{port}", nil) }
    }

    fn handleIngest(http.ResponseWriter w, http.Request r) {
        var body = readBody(r)
        var ff = FlowFile(
            uuid(),
            {"http.method": "POST", "http.path": r.URL.Path},
            body,
            int(time.Now().Unix())
        )
        output.send(ff)
        fmt.Fprintf(w, "accepted")
    }
}

// Filesystem sink — writes FlowFiles to disk
class FileSink : ProcessorFn {
    String outputDir

    init(String outputDir) {
        this.outputDir = outputDir
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var path = outputDir + "/" + ff.id + ".out"
        os.WriteFile(path, ff.content.toBytes(), 0644)
        return Drop()
    }
}
```

---

## Back-Pressure

Back-pressure propagates naturally via bounded channels:

- **Within a group**: `Channel<FlowFile>(capacity)` — `send()` blocks when full. Upstream goroutine sleeps until downstream catches up.
- **Between groups**: NATS JetStream stream limits. When limit is reached, NATS rejects publishes — upstream group backs off.

No spill-to-disk in Phase 1. Bounded channels are sufficient — it's exactly how NiFi does it.

---

## Routing Model

### Within a Group — Direct Channel Routing

The worker loop pushes to the right output channel based on ProcessorResult:

```zinc
// Processor returns Routed — worker loop routes to the named channel
class Validate : ProcessorFn {
    pub fn process(FlowFile ff): ProcessorResult {
        if ff.attributes.get("order_id") == "" {
            return Routed("invalid", ff)
        }
        return Routed("valid", ff)
    }
}
// Worker loop pushes to outputs["valid"] or outputs["invalid"]
```

### Between Groups — NATS Subject-Based Routing (Phase 2)

Cross-group routing uses NATS subjects:

```
Subject hierarchy:
  zinc-flow.{pipeline}.{group}.{route}

Examples:
  zinc-flow.orders.enrich.high-priority
  zinc-flow.orders.enrich.default
```

Routing rules use attribute-based predicates — no `eval()`, no reflection:

```zinc
enum RoutingOperator {
    EQ, NEQ, LT, GT, CONTAINS, STARTSWITH, ENDSWITH, EXISTS
}

data RoutingPredicate(
    String attribute,
    RoutingOperator operator,
    String value
)

data RoutingRule(
    String name,
    List<RoutingPredicate> predicates,
    String targetSubject,
    int priority,
    bool enabled
)
```

---

## Cross-Cutting Concerns

### Processor Configuration

Three sources, priority order:

| Priority | Source | When |
|----------|--------|------|
| Highest | State store (live overrides) | Operator changes in production |
| Medium | Pipeline definition | Developer sets at wiring time |
| Lowest | Processor defaults | Developer sets in code |

### Secrets Management

Pluggable secrets provider:

```zinc
interface SecretsProvider {
    fn get(String key): String
}

class EnvSecrets : SecretsProvider {
    pub fn get(String key): String {
        return os.Getenv(key)
    }
}
```

### Observability

Automatic — the runtime handles it. The worker loop instruments everything:

- **Logging**: structured JSON, per-processor log level
- **Metrics**: processed count, error count, queue depth, latency histogram
- **Tracing**: FlowFiles carry trace context in attributes (Phase 3, OpenTelemetry)

---

## Management API (Phase 2)

REST API for runtime control:

```zinc
class FlowAPI {
    Pipeline pipeline

    pub fn start(int port) {
        http.HandleFunc("/api/pipeline", getPipeline)
        http.HandleFunc("/api/processors", getProcessors)
        http.HandleFunc("/api/processors/start", startProcessor)
        http.HandleFunc("/api/processors/stop", stopProcessor)
        http.HandleFunc("/api/processors/scale", scaleProcessor)
        http.HandleFunc("/api/queues", getQueues)
        http.HandleFunc("/api/stats", getStats)
        http.HandleFunc("/api/health", healthCheck)
        http.ListenAndServe(":{port}", nil)
    }
}
```

---

## CLI

```bash
# Local dev — all groups in one process
zinc run .

# Build native binary
zinc build .

# Runtime management (Phase 2)
zinc flow status                                    # pipeline overview
zinc flow processors                                # per-processor stats
zinc flow processor stop enrich                     # stop a processor
zinc flow processor start enrich                    # start it
zinc flow processor scale enrich --replicas 4       # scale within group
zinc flow queues                                    # channel depths
```

---

## Open Questions — Resolved

### Q1: Queue technology
**Answer: Pluggable. Typed channels within groups. NATS JetStream between groups.**

| Concern | Tool | Why |
|---------|------|-----|
| Messaging (cross-group) | NATS JetStream | Lightweight (~20MB), consumer groups, K8s-native |
| State + audit trail | etcd or PostgreSQL | Strong consistency, revision history |
| Large content | Filesystem (local/NFS) | Zero dependencies, proven |

### Q2: GUI framework
**Answer: REST API first, TUI second, Web UI later.**

### Q3: Delivery guarantees
**Answer: At-least-once within a group (channel semantics). Configurable per NATS stream for cross-group.**

### Q4: Content repository
**Answer: Phase 2. For Phase 1, content is inline in FlowFile. For large payloads crossing groups, store on shared filesystem and pass reference.**
