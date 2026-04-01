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
    var routeTable = Map<RouteKey, Channel<FlowFile>>{}  // shared, owned by ProcessorGroup
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

    // Drain remaining items from input channel (used during removal)
    pub fn drain() {
        while len(input) > 0 {
            input.recv()  // discard, or forward to DLQ
        }
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

    // Route output via the shared routing table.
    // The table is owned by ProcessorGroup and can be mutated at any time
    // (reroute, splice). The worker reads it on each send — no restart needed.
    fn routeOutput(ProcessorResult result) {
        match result {
            case Single(ff) {
                outputChan("default").send(ff)
            }
            case Multiple(ffs) {
                for ff in ffs {
                    outputChan("default").send(ff)
                }
            }
            case Routed(route, ff) {
                outputChan(route).send(ff)
            }
            case Drop {
                // discard
            }
        }
    }

    // Look up the output channel from the routing table.
    // This indirection is what makes live rerouting possible —
    // the channel pointer can change between calls.
    fn outputChan(String output): Channel<FlowFile> {
        var key = RouteKey(name, output)
        if routeTable.containsKey(key) {
            return routeTable.get(key)
        }
        // Fallback to default output
        return routeTable.get(RouteKey(name, "default"))
    }
}
```

### ProcessorRegistry

All available processor types register at startup. Live graph mutation picks from this registry — no new code at runtime.

```zinc
class ProcessorRegistry {
    var factories = Map<String, Fn<(), ProcessorFn>>{}

    pub fn register(String name, Fn<(), ProcessorFn> factory) {
        factories.put(name, factory)
    }

    pub fn create(String name): ProcessorFn {
        var factory = factories.get(name)
        return factory()
    }

    pub fn list(): List<String> {
        return factories.keys()
    }
}
```

Usage at startup:
```zinc
var registry = ProcessorRegistry()
registry.register("enrich", () -> Enrich())
registry.register("validate", () -> ValidateOrder())
registry.register("filter-empty", () -> FilterEmpty())
registry.register("split-lines", () -> SplitLines())
```

### ProcessorGroup

Owns the routing table. All graph mutations go through ProcessorGroup under a mutex — upstream processors never pause.

```zinc
// RouteKey: (processor-name, output-name) → target channel
data RouteKey(String processor, String output)

class ProcessorGroup {
    String name
    var workers = Map<String, ProcessorWorker>{}
    var routes = Map<RouteKey, Channel<FlowFile>>{}  // the routing table
    var registry = ProcessorRegistry()
    // var mu = sync.Mutex{}  — protects routes during mutation

    // --- Build-time wiring ---

    pub fn addProcessor(String name, ProcessorFn fn) {
        workers.put(name, ProcessorWorker(name, fn))
    }

    pub fn connect(String from, String to) {
        connect(from, "default", to)
    }

    pub fn connect(String from, String output, String to) {
        var target = workers.get(to)
        routes.put(RouteKey(from, output), target.input)
        // Worker reads from routes table, not direct channel refs
        workers.get(from).routeTable = routes
    }

    // --- Lifecycle ---

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

    // --- Live graph mutation (single-pod safe) ---

    // Add a new processor from the registry and wire it in
    pub fn addLive(String name, String registryKey) {
        // mu.Lock(); defer mu.Unlock()
        var fn = registry.create(registryKey)
        var worker = ProcessorWorker(name, fn)
        workers.put(name, worker)
        worker.routeTable = routes
        worker.start()
    }

    // Remove a processor: stop it, drain its input, clean up routes
    pub fn removeLive(String name) {
        // mu.Lock(); defer mu.Unlock()
        var worker = workers.get(name)
        worker.stop()
        // Drain remaining items to DLQ or discard
        worker.drain()
        // Remove all routes that reference this processor
        for key in routes {
            if key.processor == name {
                routes.remove(key)
            }
        }
        workers.remove(name)
    }

    // Reroute: change where a processor sends its output
    // Upstream never stops — sees new target on next send()
    pub fn reroute(String from, String output, String newTarget) {
        // mu.Lock(); defer mu.Unlock()
        var target = workers.get(newTarget)
        routes.put(RouteKey(from, output), target.input)
    }

    // Splice: insert a new processor between two existing ones
    pub fn splice(String between, String and, String newName, String registryKey) {
        // mu.Lock(); defer mu.Unlock()
        var fn = registry.create(registryKey)
        var worker = ProcessorWorker(newName, fn)
        workers.put(newName, worker)
        worker.routeTable = routes

        // Point upstream's output to new processor's input
        routes.put(RouteKey(between, "default"), worker.input)
        // Point new processor's output to downstream's input
        var downstream = workers.get(and)
        routes.put(RouteKey(newName, "default"), downstream.input)

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
        // Existing lifecycle endpoints
        http.HandleFunc("/api/pipeline", getPipeline)
        http.HandleFunc("/api/processors", getProcessors)
        http.HandleFunc("/api/processors/start", startProcessor)
        http.HandleFunc("/api/processors/stop", stopProcessor)
        http.HandleFunc("/api/processors/scale", scaleProcessor)
        http.HandleFunc("/api/queues", getQueues)
        http.HandleFunc("/api/stats", getStats)
        http.HandleFunc("/api/health", healthCheck)

        // Live graph mutation endpoints
        http.HandleFunc("/api/registry", listRegistry)            // GET: available processor types
        http.HandleFunc("/api/graph/add", addProcessor)           // POST: {name, registryKey, group}
        http.HandleFunc("/api/graph/remove", removeProcessor)     // POST: {name, group}
        http.HandleFunc("/api/graph/reroute", rerouteProcessor)   // POST: {from, output, newTarget, group}
        http.HandleFunc("/api/graph/splice", spliceProcessor)     // POST: {between, and, newName, registryKey, group}
        http.HandleFunc("/api/graph", getGraph)                   // GET: current graph topology

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

# Live graph mutation (Phase 2)
zinc flow registry                                  # list available processor types
zinc flow graph                                     # show current topology
zinc flow graph add my-filter --type filter-empty   # add processor from registry
zinc flow graph connect enrich my-filter             # wire enrich → my-filter
zinc flow graph reroute enrich --to my-filter        # redirect enrich output
zinc flow graph splice enrich sink --type validate   # insert between two processors
zinc flow graph remove my-filter                     # drain and remove
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
