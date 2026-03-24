# Design: Zinc Flow Runtime — Architecture & Implementation

> **Status**: DESIGN — architecture validated, ready for Phase 1 implementation

## Design Principles

### Program to Interfaces, Not Implementations

Every major component in Zinc Flow is defined as an interface. Implementations are pluggable. Code depends on the interface, never on a concrete implementation.

This is a hard requirement — not a nice-to-have. It's what makes the system pluggable (swap NATS for Kafka), testable (mock the queue in unit tests), and evolvable (replace the content store without touching processors).

Core interfaces:

| Interface | Purpose | Implementations |
|-----------|---------|----------------|
| `FlowQueue` | Message passing between processors | `LocalQueue`, `NatsQueue`, `KafkaQueue` |
| `ContentStore` | Large FlowFile content storage | `FileContentStore`, `S3ContentStore` |
| `StateStore` | Flow graph, catalog, audit trail | `EtcdStateStore`, `PostgresStateStore`, `LocalStateStore` |
| `SecretsProvider` | Credential resolution | `EnvSecrets`, `FileSecrets`, `VaultSecrets` |
| `Router` | FlowFile routing between groups | `LocalRouter`, `NatsSubjectRouter` |
| `MetricsExporter` | Observability output | `ConsoleMetrics`, `PrometheusMetrics` |

Processors, sources, and sinks also program to interfaces — a processor takes a `FlowFile` and returns a `FlowFile`. It never knows whether the queue is in-memory or NATS, whether content is on local disk or NFS, whether secrets come from env vars or Vault.

```zinc
// Processor only depends on FlowFile — it doesn't know queue, content store, or secret details
class EnrichProcessor : ProcessorFn {
    init DataSource db

    init(DataSource db) {
        this.db = db
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var data = Json.parse(ff.content) // doesn't know if content was inline or from content store
        data.put("region", db.query("SELECT region FROM zip WHERE code = ?", data.getString("zip")))
        return new Single(ff.withContent(Json.toBytes(data)))
    }
}
```

This applies to all wiring in the runtime: `Pipeline` holds `FlowQueue` references (not `LocalQueue`), `ProcessorWorker` takes a `FlowQueue` (not `NatsQueue`), `Router` is an interface the runtime calls (not hardcoded NATS subject logic).

### Dogfood Zinc — Never Fall Back to Raw Java

Zinc Flow is written in Zinc. This is a deliberate test of the language design.

When we hit something awkward or missing while building the runtime — a pattern that's clunky, a feature that's needed, a transpiler optimization that would help — **that's a signal to improve Zinc, not to drop into raw Java.**

Examples of things we might discover we need:
- Methods on `data` classes (for `FlowFile.withContent()`)
- Named output declarations in pipeline wiring
- Generic interface implementations (`FlowQueue<FlowFile>`)
- Sealed class syntax (for `ProcessorResult` — needed for GraalVM-safe exhaustive matching)
- Pattern matching on sealed hierarchies (for `routeOutput`)
- Builder patterns or copy-and-update for immutable types

Each gap found is a language improvement tracked as a GitHub issue on the Zinc repo. The runtime is the proving ground — if Zinc can build a production data flow engine, it can build anything.

---

## Architecture Decision

**Processor Groups** — the middle ground between NiFi's monolith and DeltaFi's container-per-processor.

```
NiFi:      Everything in one JVM            → can't scale pieces independently
DeltaFi:   Every processor in a container   → IPC overhead kills small processors
Zinc Flow: YOU choose the group boundaries  → group fast processors, isolate slow ones
```

### The Model

- A **Processor Group** is the unit of deployment (one pod, one process)
- Within a group: **virtual threads + Channel\<T\>** (zero serialization, reference passing, high throughput)
- Between groups: **NATS JetStream** (serialization only at group boundaries)
- **Local dev**: all groups collapse into one process, everything is in-memory
- **K8s**: each group becomes a Deployment with its own replica count

```
Pod 1 (ingest-group, 1 replica):
  [http-source] -> [parse] -> [validate]     ← virtual threads, Channel<T>
                                    |
                             NATS JetStream   ← cross-group boundary
                                    |
Pod 2 (enrich-group, 10 replicas):            ← the slow one, scaled out
  [enrich] -> [lookup]                        ← virtual threads, Channel<T>
                  |
             NATS JetStream
                  |
Pod 3 (output-group, 2 replicas):
  [format] -> [kafka-sink]                    ← virtual threads, Channel<T>
```

### Why This Works

| Feature | JVM Advantage |
|---------|--------------|
| **Virtual threads** | Millions of concurrent threads, near-zero overhead, purpose-built for blocking I/O and queue consumption loops |
| **Channel\<T\>** (ArrayBlockingQueue) | Bounded blocking queue — `put()` blocks when full (back-pressure), `take()` blocks when empty. Thread-safe, zero-copy reference passing within a JVM |
| **JIT compilation** (JVM mode) | Long-running processor loops get faster over time as HotSpot optimizes hot paths. In native-image mode, AOT compilation trades JIT warmup for instant peak performance and ~100ms startup |
| **GraalVM native-image** | Compile to standalone binary — ~100ms startup, no JVM needed at runtime, small memory footprint |
| **Proven at scale** | NiFi runs on the JVM and handles 10K-100K msgs/sec. Zinc Flow targets the same runtime with less architectural bloat |

### Virtual Thread-Level Manageability

Virtual threads give you everything process isolation gives you for management, because the **queue is the decoupling boundary**:

```zinc
class ProcessorGroup {
    init String name
    var Map<String, ProcessorWorker> workers = {}

    // Stop a processor — thread exits, items accumulate in its queue
    pub fn stopProcessor(String procName) {
        workers.get(procName).stop()
    }

    // Start — new virtual thread picks up backlog from queue
    pub fn startProcessor(String procName) {
        workers.get(procName).start()
    }

    // Swap — stop, replace function, start. Queue bridges the gap.
    pub fn swapProcessor(String procName, ProcessorFn newFn) {
        var worker = workers.get(procName)
        worker.stop()
        worker.awaitStop()
        worker.setFn(newFn)
        worker.start()
    }

    // Scale — multiple virtual threads consuming from same queue
    pub fn scaleProcessor(String procName, int replicas) {
        workers.get(procName).setReplicas(replicas)
    }
}
```

The only thing you lose vs process isolation is crash protection — a native library segfault takes down the JVM. But Java exceptions are caught by the worker loop and routed to DLQ.

---

## Core Types

### FlowFile

```zinc
data FlowFile(
    String id,
    Map<String, String> attributes,
    byte[] content,
    long timestamp,
    List<String> provenance = []
) {
    pub fn withContent(byte[] newContent): FlowFile {
        return FlowFile(
            id,
            attributes,
            newContent,
            System.currentTimeMillis(),
            provenance + ["transformed"]
        )
    }

    pub fn withAttribute(String key, String value): FlowFile {
        var attrs = HashMap(attributes)
        attrs.put(key, value)
        return FlowFile(id, attrs, content, System.currentTimeMillis(), provenance)
    }

    pub fn contentSize(): int = content.length
}
```

FlowFiles are **immutable** — processors return new FlowFiles. Within a JVM, FlowFiles passed through `Channel<T>` are reference-passed (no serialization, no copy).

### ProcessorResult

Processors return a `ProcessorResult` — a sealed class hierarchy that the worker loop can match exhaustively without runtime reflection. GraalVM native-image handles sealed hierarchies natively (all subtypes known at build time).

```zinc
sealed class ProcessorResult {
    data Single(FlowFile ff)
    data Multiple(List<FlowFile> ffs)
    data Routed(String route, FlowFile ff)
    data object Drop
}
```

Processors return `ProcessorResult` explicitly. The sealed class hierarchy makes the return type clear — no magic wrapping, no transpiler tricks. The developer chooses which variant to return:

- `new Single(ff)` — 1:1 transform
- `new Multiple(list)` — 1:N split/fan-out
- `new Routed("name", ff)` — route to a named output
- `new Drop()` — filter/discard

The worker loop always receives `ProcessorResult` — one type to match, exhaustive and GraalVM-safe.

```zinc
// 1:1 — explicitly return Single
ProcessorFn enrich = (ff) -> {
    return new Single(ff.withAttribute("enriched", "true"))
}

// 1:N — explicitly return Multiple
ProcessorFn split = (ff) -> {
    var records = Json.parseArray(ff.content)
    return new Multiple(records.map(r -> FlowFile(...)))
}

// Routing — explicitly return Routed
ProcessorFn validate = (ff) -> {
    if ff.attributes.get("id") == null {
        return new Routed("invalid", ff.withAttribute("error", "missing id"))
    }
    return new Routed("valid", ff)
}

// Filter — explicitly return Single or Drop
ProcessorFn filterJunk = (ff) -> {
    if ff.contentSize() == 0 { return new Drop() }
    return new Single(ff)
}
```

### Content Reference (Phase 2 — Large Payloads)

Not needed in Phase 1. The JVM handles byte arrays through in-memory queues efficiently. Content references are for the Phase 2 distributed case where FlowFiles cross pod boundaries and you don't want to serialize 10MB through NATS.

```zinc
data ContentRef(
    String store,   // "file", "nfs", "s3-compat"
    String key,     // content identifier
    long size,      // byte length
    String hash     // sha256 for dedup/integrity
)
```

When a FlowFile crosses a group boundary, the runtime decides:
- **< 256KB**: serialize inline through NATS message (fast enough)
- **>= 256KB**: store content on shared filesystem, pass ContentRef through NATS

Content store options by deployment:
- **Local dev**: local filesystem (`/tmp/zinc-flow/content/`)
- **Single node prod**: local filesystem
- **Multi-node K8s**: shared filesystem via NFS or K8s `ReadWriteMany` PVC (EFS, Azure Files, etc.)
- **Future**: pluggable interface for S3-compatible stores (SeaweedFS, RustFS, Garage) when needed

**Explicitly not supported**: Rook/Ceph. Too complex, too heavy, too many failure modes for what is essentially a temporary blob cache.

NiFi uses local filesystem for its content repository and it works at scale. Keep it simple.

---

## Processor Model

### Processor Definition

A processor is a function that implements `ProcessorFn` — either as a lambda or a class. It takes a `FlowFile` and returns a `ProcessorResult`:

```zinc
import flow

// Simple processor as a lambda
ProcessorFn enrich_order = (ff) -> {
    var data = Json.parse(ff.content)
    data.put("enriched_at", Instant.now().toString())
    data.put("region", lookup_region(data.getString("zip_code")))
    return new Single(ff.withContent(Json.toBytes(data)))
}
```

Processors return `ProcessorResult` explicitly:
- `new Single(ff)` — 1:1 transform
- `new Multiple(list)` — 1:N split/fan-out
- `new Drop()` — filter/discard
- `new Routed("name", ff)` — route to a named output

```zinc
// 1:N — split a batch into individual records
ProcessorFn split_batch = (ff) -> {
    var records = Json.parseArray(ff.content)
    return new Multiple(records.mapIndexed((r, i) ->
        FlowFile(
            UUID.randomUUID().toString(),
            ff.attributes + {"record.index": i.toString()},
            Json.toBytes(r),
            System.currentTimeMillis()
        )
    ))
}

// Filter — return Drop to discard
ProcessorFn filter_valid = (ff) -> {
    var data = Json.parse(ff.content)
    if data.getString("status") == "invalid" {
        return new Drop()
    }
    return new Single(ff)
}

// Routing — named outputs declared in pipeline wiring, processor returns Routed
ProcessorFn validate_order = (ff) -> {
    var data = Json.parse(ff.content)
    if data.getString("order_id") == null {
        return new Routed("failure", ff.withAttribute("error", "missing order_id"))
    }
    if data.getInt("amount", 0) <= 0 {
        return new Routed("retry", ff.withAttribute("error", "invalid amount"))
    }
    return new Routed("success", ff)
}
```

### Processor Lifecycle

Each processor runs as a **worker loop** consuming from its input queue using virtual threads:

```zinc
class ProcessorWorker {
    init String name
    var ProcessorFn fn
    init FlowQueue inputQueue
    init Map<String, FlowQueue> outputQueues
    var String state = "stopped"  // stopped, running, paused
    var List<Thread> threads = []
    var Map<String, String> config = {}
    var ProcessorStats stats = ProcessorStats()

    pub fn start() {
        state = "running"
        if threads.isEmpty() {
            addThread()
        }
    }

    pub fn stop() {
        state = "stopped"
        for t in threads {
            t.join()
        }
        threads.clear()
    }

    pub fn awaitStop() {
        for t in threads {
            t.join()
        }
    }

    pub fn setReplicas(int n) {
        while threads.size() < n {
            addThread()
        }
        while threads.size() > n {
            threads.removeLast()
        }
    }

    fn addThread() {
        var t = spawn {
            runLoop()
        }
        threads.add(t)
    }

    fn runLoop() {
        while state == "running" {
            var ff = inputQueue.poll(100)  // 100ms timeout
            if ff == null {
                continue
            }

            var startTime = System.nanoTime()
            var result = execute(ff) or {
                handleFailure(ff, err)
                continue
            }

            var elapsed = (System.nanoTime() - startTime) / 1_000_000.0
            stats.record(elapsed, ff.contentSize())
            routeOutput(result)
        }
    }

    fn execute(FlowFile ff): ProcessorResult {
        var maxRetries = Integer.parseInt(config.getOrDefault("max_retries", "0"))
        for attempt in 0..=maxRetries {
            var result = fn(ff) or {
                if attempt < maxRetries {
                    var delay = Long.parseLong(config.getOrDefault("retry_delay", "1000")) * (1L << attempt)
                    Thread.sleep(delay)
                    continue
                }
                return Error(err)
            }
            return result
        }
    }

    fn routeOutput(ProcessorResult result) {
        match result {
            ProcessorResult.Single(ff) -> {
                outputQueues.get("default").put(ff)
            }
            ProcessorResult.Multiple(ffs) -> {
                for ff in ffs {
                    outputQueues.get("default").put(ff)
                }
            }
            ProcessorResult.Routed(route, ff) -> {
                outputQueues.getOrDefault(route, outputQueues.get("default")).put(ff)
            }
            ProcessorResult.Drop -> {
                // Dropped
            }
        }
    }

    fn handleFailure(FlowFile ff, Error err) {
        stats.recordError()
        if outputQueues.containsKey("dead_letter") {
            var dlqFf = ff.withAttribute("error", err.toString())
                          .withAttribute("error.processor", name)
                          .withAttribute("error.timestamp", Instant.now().toString())
            outputQueues.get("dead_letter").put(dlqFf)
        }
    }
}
```

---

## Pipeline Definition

### Pipeline DSL

```zinc
import flow

ProcessorFn parse_json = (ff) -> { ... }

ProcessorFn validate = (ff) -> { ... }

ProcessorFn enrich = (ff) -> { ... }

ProcessorFn format_output = (ff) -> { ... }

// --- Pipeline with processor groups ---

var pipeline = flow.Pipeline("order_processing")

// Group 1: ingest (lightweight, 1 replica)
var ingest = flow.Group("ingest") {
    flow.source.http(port = 8080)
    -> parse_json
    -> validate
}

// Group 2: enrichment (slow, needs scaling)
var enrichGroup = flow.Group("enrich", replicas = 10) {
    enrich
}

// Group 3: output
var output = flow.Group("output", replicas = 2) {
    format_output
    -> flow.sink.kafka("processed-orders")
}

// Connect groups (these become distributed queues)
pipeline.connect(ingest, enrichGroup)
pipeline.connect(enrichGroup, output)

pipeline.run()
```

### Local Dev Mode

In local dev, group boundaries are ignored — everything runs as virtual threads in one process with in-memory queues:

```bash
# Local — all groups in one process, in-memory queues
zinc flow run pipeline.zn

# K8s — each group is a Deployment, NATS JetStream between groups
zinc flow deploy pipeline.zn --nats nats://nats:4222
```

### Pipeline Object

```zinc
class Pipeline {
    init String name
    var Map<String, ProcessorGroup> groups = {}
    var List<GroupConnection> groupConnections = []
    var String mode = "local"  // "local" or "distributed"

    pub fn connect(ProcessorGroup source, ProcessorGroup target) {
        groupConnections.add(GroupConnection(source.name, target.name))
    }

    pub fn run() {
        if mode == "local" {
            runLocal()
        } else {
            runDistributed()
        }
    }

    fn runLocal() {
        // Collapse all groups into one process
        // All connections use in-memory queues
        print("Pipeline '{name}' starting (local mode)")
        for group in groups.values() {
            for worker in group.workers.values() {
                worker.start()
            }
        }
        waitForShutdown()
    }

    fn runDistributed() {
        // Only start this process's group
        // Cross-group connections use NATS JetStream
        var myGroup = System.getenv("ZINC_FLOW_GROUP")
        if myGroup != null {
            groups.get(myGroup).start()
        }
    }

    pub fn status(): Map<String, ProcessorStatus> {
        var result = HashMap<String, ProcessorStatus>()
        for (groupName, group) in groups {
            for (procName, worker) in group.workers {
                result.put("{groupName}/{procName}", {
                    "state": worker.state,
                    "queue_depth": worker.inputQueue.size(),
                    "processed": worker.stats.count,
                    "errors": worker.stats.errors,
                    "avg_ms": worker.stats.avgLatencyMs,
                    "msgs_per_sec": worker.stats.throughput,
                    "replicas": worker.threads.size()
                })
            }
        }
        return result
    }
}
```

---

## Queue Abstraction

The queue backend is pluggable — same interface, different implementations:

```zinc
interface FlowQueue {
    fn put(FlowFile ff)
    fn poll(long timeoutMs): FlowFile?
    fn size(): int
}

// In-memory (within a group) — Channel<T> backed by ArrayBlockingQueue
class LocalQueue : FlowQueue {
    var Channel<FlowFile> channel

    pub fn init(int maxSize = 10_000) {
        channel = Channel(maxSize)
    }

    pub fn put(FlowFile ff) {
        channel.send(ff)  // blocks when full → natural back-pressure
    }

    pub fn poll(long timeoutMs): FlowFile? {
        return channel.receive(timeoutMs) or { return null }
    }

    pub fn size(): int = channel.size()
}

// NATS JetStream (between groups) — cross-pod communication
class NatsQueue : FlowQueue {
    var Connection nc
    var JetStream js
    init String stream
    init String subject
    init String consumerName
    var JetStreamSubscription sub

    pub fn init(String natsUrl, String stream, String subject, String consumerName) {
        nc = Nats.connect(natsUrl)
        js = nc.jetStream()

        // Create stream if not exists
        js.addStream(StreamConfiguration.builder()
            .name(stream)
            .subjects(subject)
            .build()) or { }

        // Create durable consumer (competing consumers for scaling)
        sub = js.subscribe(subject, PullSubscribeOptions.builder()
            .durable(consumerName)
            .build())
    }

    var ContentStore? contentStore  // for large payloads

    pub fn put(FlowFile ff) {
        if ff.contentSize() < 256 * 1024 or contentStore == null {
            // Small: inline in NATS message
            var data = FlowFileSerde.serialize(ff)
            js.publish(subject, data)
        } else {
            // Large: store content on shared filesystem, pass reference
            var key = "{ff.id}-content"
            contentStore.put(key, ff.content)
            var lightFf = ff.withContent(key.getBytes())
            var data = FlowFileSerde.serialize(lightFf)
            js.publish(subject, data, Headers().add("Content-Ref", "true"))
        }
    }

    pub fn poll(long timeoutMs): FlowFile? {
        var msgs = sub.fetch(1, Duration.ofMillis(timeoutMs))
        if msgs.isEmpty() {
            return null
        }
        var msg = msgs.get(0)
        var ff = FlowFileSerde.deserialize(msg.getData())

        if msg.getHeaders() != null and msg.getHeaders().get("Content-Ref") == "true" {
            // Retrieve large content from shared filesystem
            var key = String(ff.content)
            ff = ff.withContent(contentStore.get(key))
            contentStore.delete(key)  // consumed, clean up
        }

        msg.ack()
        return ff
    }

    pub fn size(): int {
        // NATS consumer info provides pending count
        return sub.getConsumerInfo().getNumPending().toInt()
    }
}
```

---

## Back-Pressure

Back-pressure propagates naturally via bounded queues:

```zinc
data BackPressureConfig(
    int queueDepthWarn = 5_000,       // log warning
    int queueDepthThrottle = 8_000,   // slow upstream
    int queueDepthBlock = 10_000      // block upstream put() call
)
```

- **Within a group**: `Channel<T>` is bounded — `send()` blocks when full. Upstream virtual thread sleeps until downstream catches up. Zero overhead, built into the JVM's blocking queue semantics.
- **Between groups**: NATS JetStream stream limits. Configure max messages or max bytes on the stream. When limit is reached, NATS rejects publishes — upstream group backs off.

No spill-to-disk in Phase 1. The bounded queue is sufficient — it's exactly how NiFi does it.

---

## Routing Model

Routing determines which queue a FlowFile goes to after a processor finishes with it. Two levels:

### Within a Group — Direct Queue Routing

Inside a group, routing is local — the worker loop pushes to the right output queue based on the processor's return value. Fast, no network, no serialization.

```zinc
// Processor returns a Routed result — named outputs declared in pipeline wiring
ProcessorFn validate = (ff) -> {
    if ff.attributes.get("order_id") == null {
        return new Routed("invalid", ff)
    }
    return new Routed("valid", ff)
}
// Worker loop pushes to outputQueues["valid"] or outputQueues["invalid"]
```

This handles static routing within a group. No routing table needed — the wiring is defined when you `group.connect()`.

### Between Groups — NATS Subject-Based Routing

Cross-group routing uses NATS subjects. This is where NATS shines — subjects are hierarchical and support wildcards, giving us content-based routing for free.

```
Subject hierarchy:
  zinc-flow.{pipeline}.{group}.{route}

Examples:
  zinc-flow.orders.enrich.high-priority
  zinc-flow.orders.enrich.default
  zinc-flow.orders.archive.csv
  zinc-flow.orders.archive.json
```

A **routing table** in the state store maps FlowFile attributes to NATS subjects. This table is updatable in production without redeploying processors.

Routing rules use an **attribute-based predicate model** inspired by the IRS backbone service — no `eval()`, no reflection, fully GraalVM native-image compatible. Rules are data (serializable JSON), not code.

```zinc
// Operators — enum, no reflection needed
enum RoutingOperator {
    EQ, NEQ, LT, GT, GTEQ, LTEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS
}

// A single predicate: attribute + operator + value
data RoutingPredicate(
    String attribute,            // FlowFile attribute key
    RoutingOperator operator,    // comparison operator
    String value = ""            // comparison value (unused for EXISTS)
)

// Predicates can be combined with AND/OR
enum RuleJoiner { AND, OR }

data RoutingRule(
    String name,
    List<RoutingPredicate> predicates,   // conditions to evaluate
    RuleJoiner joiner = RuleJoiner.AND,  // how to combine predicates
    String targetSubject,                // NATS subject to publish to
    int priority = 0,                    // higher priority rules evaluated first
    boolean enabled = true               // can be toggled at runtime
)

// Example routing table (stored in state store, editable via API/CLI)
var rules = [
    RoutingRule(
        "high-priority",
        [RoutingPredicate("priority", RoutingOperator.EQ, "high")],
        RuleJoiner.AND,
        "zinc-flow.orders.enrich.high",
        10
    ),
    RoutingRule(
        "csv-files",
        [RoutingPredicate("mime.type", RoutingOperator.EQ, "text/csv")],
        RuleJoiner.AND,
        "zinc-flow.orders.csv-processing.default",
        5
    ),
    RoutingRule(
        "large-eu-orders",
        [
            RoutingPredicate("region", RoutingOperator.EQ, "eu"),
            RoutingPredicate("amount", RoutingOperator.GT, "1000"),
        ],
        RuleJoiner.AND,
        "zinc-flow.orders.eu-processing.default",
        8
    ),
    RoutingRule(
        "default",
        [],  // empty predicates = always matches
        RuleJoiner.AND,
        "zinc-flow.orders.enrich.default",
        0
    ),
]
```

For complex routing logic that can't be expressed as predicates (e.g., database lookups, multi-step decisions), use a processor function — compiled at build time, AOT-safe. Named outputs are declared in the pipeline wiring:

```zinc
// Named outputs ["high", "normal", "low"] declared in pipeline wiring
ProcessorFn routeByBusinessLogic = (ff) -> {
    // Complex routing stays as compiled code, not runtime-evaluated strings
    var amount = Integer.parseInt(ff.attributes.getOrDefault("amount", "0"))
    var region = ff.attributes.getOrDefault("region", "unknown")
    if amount > 10000 and region == "us-east" {
        return new Routed("high", ff)
    }
    return new Routed("normal", ff)
}
```

Consumer groups subscribe with wildcards for natural scaling:
- `enrich-group` subscribes to `zinc-flow.orders.enrich.>` — receives all enrich traffic
- All 10 replicas compete for messages via NATS consumer groups
- Adding a new route just means publishing to a new subject — consumers pick it up automatically if the wildcard matches

### Route Evaluation

The runtime evaluates routing rules when a FlowFile exits a group's last processor. All evaluation is simple attribute lookups and comparisons — no expression parsing, no reflection, fully AOT-compatible:

```zinc
class Router {
    var List<RoutingRule> rules
    init String defaultSubject
    init String failureSubject   // IRS-inspired: route here when no rules match

    pub fn route(FlowFile ff): String {
        // Rules sorted by priority (highest first), only enabled rules
        for rule in rules {
            if rule.enabled and evaluateRule(rule, ff) {
                return rule.targetSubject
            }
        }
        return defaultSubject
    }

    fn evaluateRule(RoutingRule rule, FlowFile ff): boolean {
        if rule.predicates.isEmpty() { return true }  // empty = always match
        var results = rule.predicates.map(p -> evaluatePredicate(p, ff))
        return match rule.joiner {
            RuleJoiner.AND -> results.allMatch(it)
            RuleJoiner.OR  -> results.anyMatch(it)
        }
    }

    fn evaluatePredicate(RoutingPredicate pred, FlowFile ff): boolean {
        var attrValue = ff.attributes.get(pred.attribute)
        if attrValue == null {
            return pred.operator == RoutingOperator.EXISTS and false
        }
        return match pred.operator {
            RoutingOperator.EQ         -> attrValue == pred.value
            RoutingOperator.NEQ        -> attrValue != pred.value
            RoutingOperator.CONTAINS   -> attrValue.contains(pred.value)
            RoutingOperator.STARTSWITH -> attrValue.startsWith(pred.value)
            RoutingOperator.ENDSWITH   -> attrValue.endsWith(pred.value)
            RoutingOperator.EXISTS     -> true  // attribute exists
            RoutingOperator.LT         -> attrValue.compareTo(pred.value) < 0
            RoutingOperator.GT         -> attrValue.compareTo(pred.value) > 0
            RoutingOperator.GTEQ       -> attrValue.compareTo(pred.value) >= 0
            RoutingOperator.LTEQ       -> attrValue.compareTo(pred.value) <= 0
        }
    }
}
```

### Dynamic Routing Changes

Routing rules live in the state store and can be changed in production:

```bash
# Add a new routing rule — takes effect immediately
zinc flow route add --name "eu-traffic" \
    --condition 'ff.attributes.get("region") == "eu"' \
    --target "zinc-flow.orders.eu-processing.default" \
    --priority 8

# List current rules
zinc flow routes
  [10] high-priority  → zinc-flow.orders.enrich.high
  [ 8] eu-traffic     → zinc-flow.orders.eu-processing.default
  [ 5] csv-files      → zinc-flow.orders.csv-processing.default
  [ 0] default        → zinc-flow.orders.enrich.default

# Remove a rule
zinc flow route remove eu-traffic

# All changes are versioned in the state store — rollback works
```

---

## Cross-Cutting Concerns

Three categories of cross-cutting concerns that span all processors:

### 1. Shared Services

Processors often need shared infrastructure — database connections, HTTP clients, SSL contexts. Instead of each processor managing its own, a service registry provides shared instances.

**Service Registry**: A typed service registry provides shared instances to processors. Services are registered at startup in the wiring code and accessed via the registry or passed directly via constructor injection. No DI container — all wiring is explicit and readable:

```zinc
class ServiceRegistry {
    var Map<String, Object> services = {}

    pub fn <T> register(String name, T service) {
        services.put(name, service)
    }

    pub fn <T> get(String name, Class<T> type) T {
        return type.cast(services.get(name))
    }
}

// Register shared services at pipeline startup
var services = ServiceRegistry()
services.register("db", PostgresPool(secrets.get("DB_URL"), 10))
services.register("http", HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build())
services.register("cache", RedisClient(secrets.get("REDIS_URL")))
```

Processors access services via constructor injection or the registry:

```zinc
// Constructor injection — preferred for processors that need services
class EnrichProcessor : ProcessorFn {
    init DataSource db

    init(DataSource db) {
        this.db = db
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var result = db.query("SELECT region FROM customers WHERE id = ?", ff.attributes.get("customer_id"))
        return new Single(ff.withAttribute("region", result.getString("region")))
    }
}

// Wiring — services passed explicitly at construction time
var db = PostgresPool(secrets.get("DB_URL"), 10)
var enrichProcessor = new EnrichProcessor(db)
group.addProcessor("enrich", enrichProcessor)
```

Services are shared across all processors in a group (same process, same connection pool). Between groups, each group has its own service instances.

### 2. Processor Configuration

Processors need configuration (API URLs, thresholds, batch sizes, feature flags) that's separate from their code and changeable without redeployment.

#### Three Sources, Priority Order

| Priority | Source | When it's set | Example |
|----------|--------|--------------|---------|
| **Highest** | State store (live overrides) | Operator changes in production via CLI/API | `zinc flow config set enrich timeout_sec 60` |
| **Medium** | Pipeline definition | Developer sets at wiring time | `group.addProcessor("enrich", enrich, config = {...})` |
| **Lowest** | Processor defaults | Developer sets in ProcessorGroup | `ProcessorGroup("name", fn).withConfig({"timeout_sec": "30"})` |

Resolution: state store override > pipeline definition > processor defaults. Secrets (`${secrets.KEY}`) resolved separately via `SecretsProvider` after config merge.

```zinc
// Processor uses config passed via ProcessorContext
ProcessorFn enrich = (ff, ctx) -> {
    var url = ctx.config.get("api_url")        // config values are String
    var timeout = Integer.parseInt(ctx.config.get("timeout_sec"))
    var key = ctx.config.get("api_key")        // resolved from secrets provider
    var result = httpGet(url, {"Authorization": key}, timeout)
    return new Single(ff.withContent(result))
}

// Config defaults set in ProcessorGroup wiring
group.addProcessor("enrich", enrich, config = {
    "batch_size": "100",
    "api_url": "https://api.example.com",
    "timeout_sec": "30",
    "retry_count": "3",
    "api_key": "${secrets.ENRICHMENT_API_KEY}",
})

// Pipeline overrides at wiring time
group.addProcessor("enrich", enrich, config = {
    "api_url": "https://api-staging.example.com",
    "batch_size": "50",
})
```

#### Live Config Changes

Operators update config in production — takes effect immediately, no restart:

```bash
# Set a config value — immediate, versioned
zinc flow config set enrich timeout_sec 60
zinc flow config set enrich api_url https://api-v2.example.com

# View current config (merged from all 3 sources)
zinc flow config show enrich
  batch_size:   50         (pipeline)
  api_url:      https://api-v2.example.com  (state store override)
  timeout_sec:  60         (state store override)
  retry_count:  3          (processor default)
  api_key:      ********   (secret)

# View config history
zinc flow config history enrich
  Rev 12  2026-03-19 14:30  vrjoshi  set timeout_sec=60
  Rev 11  2026-03-19 14:25  vrjoshi  set api_url=https://api-v2.example.com

# Reset an override — fall back to pipeline/default value
zinc flow config reset enrich timeout_sec
```

Config changes are versioned in the state store — same audit trail as processor swaps and routing changes. Rollback works across all of them.

#### Implementation

The `ProcessorWorker` reads config on initialization and when notified of changes. The worker doesn't need to restart — it reads `ctx.config` on each invocation, which is a reference to the merged config map. When the state store changes, the runtime swaps the reference atomically (volatile reference swap — thread-safe on the JVM).

```zinc
class ProcessorConfig {
    var Map<String, String> defaults         // from ProcessorGroup config
    var Map<String, String> pipelineConfig   // from group.addProcessor()
    var Map<String, String> overrides        // from state store (live changes)

    pub fn resolve(SecretsProvider secrets): Map<String, String> {
        // Merge: defaults < pipeline < overrides
        var merged = HashMap(defaults)
        merged.putAll(pipelineConfig)
        merged.putAll(overrides)

        // Resolve ${secrets.KEY} references
        for (key, value) in merged {
            if value.startsWith("\${secrets.") {
                var secretKey = value.substring(10, value.length() - 1)
                merged.put(key, secrets.get(secretKey))
            }
        }
        return merged
    }
}
```

### 3. Secrets Management

Processors need credentials — API keys, database passwords, TLS certs. These must never be hardcoded or stored in the pipeline definition.

A pluggable **secrets provider** resolves secret references at runtime:

```zinc
interface SecretsProvider {
    fn get(String key): String
}

// Implementations
class EnvSecrets : SecretsProvider {
    // Reads from environment variables — simplest, K8s-native
    pub fn get(String key): String {
        return System.getenv(key)
    }
}

class FileSecrets : SecretsProvider {
    // Reads from files — mounted K8s secrets
    init String basePath = "/var/run/secrets"

    pub fn get(String key): String {
        return Files.readString(Path.of(basePath, key)).strip()
    }
}

class VaultSecrets : SecretsProvider {
    // Reads from HashiCorp Vault
    var VaultClient client

    pub fn get(String key): String {
        return client.read("secret/data/zinc-flow/{key}").getData().get("value")
    }
}
```

Processor config references secrets with `${secrets.KEY}` syntax, resolved at startup:

```zinc
// Config with secret references set in ProcessorGroup wiring
group.addProcessor("enrich", enrich, config = {
    "api_key": "${secrets.ENRICHMENT_API_KEY}",
    "db_url": "${secrets.DB_URL}",
})

// Processor reads resolved config at runtime
ProcessorFn enrich = (ff, ctx) -> {
    var key = ctx.config.get("api_key")   // resolved from secrets provider
    var result = httpGet("https://api.example.com/enrich", {"Authorization": key})
    return new Single(ff.withContent(result))
}
```

Provider chain: try Vault first, fall back to file secrets, fall back to env vars. Configurable per deployment.

```bash
# Local dev — env vars
export ENRICHMENT_API_KEY=dev-key-123
zinc flow run pipeline.zn

# K8s — mounted secrets
zinc flow run pipeline.zn --secrets file:///var/run/secrets

# Prod — Vault
zinc flow run pipeline.zn --secrets vault://vault:8200/secret/zinc-flow
```

### 3. Observability (Logging, Metrics, Telemetry)

Observability is **automatic** — the runtime handles it. Processors don't need to add logging or metrics code. The worker loop instruments everything.

#### Logging

Every FlowFile enter/exit is logged automatically by the worker loop:

```zinc
// Inside ProcessorWorker.runLoop() — automatic, not user code
fn runLoop() {
    while state == "running" {
        var ff = inputQueue.poll(100)
        if ff == null { continue }

        log.debug("processor={name} action=enter flowfile={ff.id} attrs={ff.attributes}")

        var startTime = System.nanoTime()
        var result = execute(ff) or {
            log.error("processor={name} action=error flowfile={ff.id} error={err}")
            handleFailure(ff, err)
            continue
        }

        var elapsed = (System.nanoTime() - startTime) / 1_000_000.0
        log.debug("processor={name} action=exit flowfile={ff.id} elapsed_ms={elapsed}")

        stats.record(elapsed, ff.contentSize())
        routeOutput(result)
    }
}
```

Structured logging (JSON) by default. Log level configurable per processor:

```bash
zinc flow log-level parse_json DEBUG    # verbose for one processor
zinc flow log-level enrich ERROR        # quiet for another
```

#### Metrics

Automatic Prometheus metrics emitted via Micrometer — zero processor code needed:

```
# Counters
zinc_flow_processed_total{pipeline="orders", group="main", processor="enrich"}
zinc_flow_errors_total{pipeline="orders", group="main", processor="enrich"}
zinc_flow_bytes_total{pipeline="orders", group="main", processor="enrich"}

# Gauges
zinc_flow_queue_depth{pipeline="orders", group="main", queue="enrich_input"}
zinc_flow_processor_state{pipeline="orders", group="main", processor="enrich"}  # 0=stopped, 1=running
zinc_flow_replicas{pipeline="orders", group="main", processor="enrich"}

# Histograms
zinc_flow_processing_duration_seconds{pipeline="orders", group="main", processor="enrich"}
zinc_flow_flowfile_size_bytes{pipeline="orders", group="main", processor="enrich"}
```

Exposed via `/metrics` endpoint on the management API. Plugs into existing Grafana/alerting stacks.

#### Distributed Tracing (Phase 3)

FlowFiles carry a trace context in their attributes as they move through the pipeline:

```zinc
// Automatic — runtime adds trace context to every FlowFile
ff.attributes.put("trace.id", "abc123")
ff.attributes.put("trace.span.id", "def456")
ff.attributes.put("trace.parent.id", "ghi789")
```

When a FlowFile crosses a group boundary (via NATS), the trace context propagates. OpenTelemetry exporter sends spans to Jaeger/Zipkin/etc. You can trace a single FlowFile's journey through the entire pipeline across groups and pods.

### Interceptor Model

All three concerns (services, secrets, observability) are implemented as **interceptors** on the worker loop — not as processor code. The processor function stays clean:

```zinc
// What the developer writes — clean business logic only
class EnrichProcessor : ProcessorFn {
    init DataSource db

    init(DataSource db) {
        this.db = db
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var data = Json.parse(ff.content)
        data.put("region", db.query("SELECT region FROM zip WHERE code = ?", data.getString("zip")))
        return new Single(ff.withContent(Json.toBytes(data)))
    }
}

// What the runtime wraps it with — automatic (via ProcessorWorker)
//   → resolve secrets into config
//   → log enter/exit
//   → emit metrics
//   → propagate trace context
//   → handle errors → DLQ
//   → record stats
```

The developer writes business logic. The runtime handles everything else.

---

## Sources and Sinks

Sources produce FlowFiles into the pipeline. Sinks consume them out. Both run on their own virtual thread within their group.

Sources receive a `Channel<FlowFile>` output queue and push FlowFiles into it. No generators or `yield` needed — Java doesn't have them, and the push model is a better fit for GraalVM native-image (no coroutine/continuation machinery).

```zinc
// HTTP source — pushes received requests into output channel
class HttpSource : FlowSource {
    init int port = 8080
    init String path = "/ingest"

    pub fn start(Channel<FlowFile> output) {
        var server = HttpServer(port)
        server.post(path, (request) -> {
            var body = request.body()
            var contentType = request.header("Content-Type", "")

            if contentType == "application/flowfile-v3" {
                var (attrs, content) = FlowFileSerde.unpackage(body)
                output.send(FlowFile(UUID.randomUUID().toString(), attrs, content, System.currentTimeMillis()))
            } else {
                output.send(FlowFile(
                    UUID.randomUUID().toString(),
                    {"http.method": "POST", "http.path": request.path(), "mime.type": contentType},
                    body,
                    System.currentTimeMillis()
                ))
            }
        })
    }
}

// Kafka source — consumes topic, pushes into output channel
class KafkaSource : FlowSource {
    init String brokers
    init String topic
    init String group

    pub fn start(Channel<FlowFile> output) {
        var consumer = KafkaConsumer(brokers, topic, group)
        for msg in consumer {
            output.send(FlowFile(
                UUID.randomUUID().toString(),
                {"kafka.topic": msg.topic(), "kafka.partition": msg.partition().toString(), "kafka.offset": msg.offset().toString()},
                msg.value(),
                System.currentTimeMillis()
            ))
        }
    }
}

// Filesystem sink — implements ProcessorFn, config via constructor
class FileSink : ProcessorFn {
    init String basePath

    init(String basePath) {
        this.basePath = basePath
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var filename = ff.attributes.getOrDefault("filename", ff.id)
        var path = Path.of(basePath, filename)
        Files.createDirectories(path.getParent())
        Files.write(path, ff.content)
        return new Drop()
    }
}

// HTTP sink — implements ProcessorFn, config via constructor
class HttpSink : ProcessorFn {
    init String url
    init HttpClient client

    init(String url) {
        this.url = url
        this.client = HttpClient.newHttpClient()
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", ff.attributes.getOrDefault("mime.type", "application/octet-stream"))
            .POST(HttpRequest.BodyPublishers.ofByteArray(ff.content))
            .build()
        client.send(request, HttpResponse.BodyHandlers.discarding())
        return new Drop()
    }
}
```

---

## Management API

REST API for runtime control. Any UI (web, TUI, CLI) is a client for this.

```zinc
class FlowAPI {
    var Pipeline pipeline

    pub fn init(Pipeline pipeline, int port = 8081) {
        var app = HttpServer(port)
        app.get("/api/pipeline",                  getPipeline)
        app.get("/api/groups",                    getGroups)
        app.get("/api/processors",                getProcessors)
        app.post("/api/processors/{name}/start",  startProcessor)
        app.post("/api/processors/{name}/stop",   stopProcessor)
        app.post("/api/processors/{name}/scale",  scaleProcessor)
        app.post("/api/processors/{name}/swap",   swapProcessor)
        app.post("/api/processors/{name}/config",  updateConfig)
        app.get("/api/queues",                    getQueues)
        app.get("/api/stats",                     getStats)
        app.get("/api/health",                    healthCheck)
    }
}
```

---

## CLI

```bash
# Local dev — all groups in one process
zinc flow run pipeline.zn

# Distributed — specify NATS server
zinc flow run pipeline.zn --mode distributed --nats nats://localhost:4222

# Deploy to K8s — generates Deployments per group
zinc flow deploy pipeline.zn --namespace prod --nats nats://nats:4222

# Runtime management
zinc flow status                                    # pipeline overview
zinc flow groups                                    # group status + replicas
zinc flow processors                                # per-processor stats
zinc flow processor stop enrich                     # stop a processor
zinc flow processor start enrich                    # start it
zinc flow processor scale enrich --replicas 4       # scale within group
zinc flow group scale enrich-group --replicas 10    # scale the K8s deployment
zinc flow queues                                    # queue depths
```

---

## Open Questions — Resolved

### Q1: Queue technology — Redis Streams? Kafka? NATS? Custom?

**Answer: Pluggable. NATS JetStream for messaging. Separate tools for state and content.**

- **Within groups**: always `Channel<T>` (in-memory, no choice needed)
- **Between groups**: NATS JetStream for message transport

| Concern | Tool | Why |
|---------|------|-----|
| **Messaging** (cross-group queues) | NATS JetStream | Lightweight (~20MB), consumer groups, purpose-built for messaging, K8s-native |
| **State + audit trail** (flow graph, config, history) | etcd or PostgreSQL | Strong consistency, proven revision history, read-your-writes guaranteed |
| **Large content** (FlowFiles > 256KB crossing groups) | Filesystem (local/NFS) | Zero dependencies, proven (NiFi uses filesystem too). Pluggable interface for future S3-compatible options (SeaweedFS, RustFS, Garage, etc.) |

Why not all-in-one NATS:
- NATS KV has no read-your-writes guarantee, max 64 history entries per key
- NATS Object Store has broken listing at scale (3.5+ sec per item)
- Jepsen found write loss under coordinated failures (2-min flush interval)
- Single point of failure if NATS handles messaging + state + content

Each tool does what it's best at. NATS dying doesn't lose your flow state. Filesystem content survives NATS restarts.

Why NATS JetStream for messaging specifically:
- **Purpose-built for messaging** — unlike Redis (cache first) or Kafka (distributed log first)
- **Lightweight single binary** — ~20MB, starts in milliseconds
- **Consumer groups** — competing consumers for scaling processor groups across pods
- **At-least-once AND exactly-once** — configurable per stream
- **Cloud-native** — designed for K8s, automatic clustering
- **Good Java client** — `io.nats:jnats` with full JetStream support

Redis Streams and Kafka available as pluggable alternatives for teams that already have them.

### Q2: GUI framework — Web UI vs TUI vs both?

**Answer: REST API first, TUI second, Web UI later.**

- Phase 1: CLI only (`zinc flow status/processors/queues`)
- Phase 2: REST API (the management API above) — enables any UI
- Phase 2: TUI using the REST API — terminal dashboard showing pipeline graph, queue depths, throughput. Fits the CLI-first Zinc philosophy.
- Phase 3+: Web UI if there's demand. Not a priority — most operators are comfortable with CLI/TUI, and web UIs are expensive to build and maintain.

### Q3 + Q4: Processor discovery, versioning, and hot-swap

**Answer: Two modes — static imports for dev, processor catalog for prod. Full audit trail with rollback.**

#### Dev Mode — Static Imports

For local development, processors are Zinc functions imported explicitly. Fast iteration, no infrastructure needed.

```zinc
// pipeline.zn
import flow
import processors.enrichment.enrich_order
import processors.validation.validate_order

var pipeline = flow.Pipeline("orders")
var group = flow.Group("main")
group.addProcessor("validate", validate_order)
group.addProcessor("enrich", enrich_order)
```

#### Prod Mode — Processor Catalog

In production, processors are registered in a **catalog** (stored in the state store — etcd or PostgreSQL). The runtime loads processors by name and version at startup, and can hot-swap them without redeployment.

This is critical for NiFi-like operations — operators need to rewire flows in production without going through a full dev/test/deploy cycle. That immediate response capability is NiFi's killer feature.

```bash
# Publish a processor to the catalog
zinc flow processor publish enrich_order --version 1.0 --package ./processors/enrichment/
zinc flow processor publish enrich_order --version 2.1 --package ./processors/enrichment_v2/

# List available processors
zinc flow processor list
  enrich_order     v1.0, v2.1
  validate_order   v1.0
  parse_json       v1.0, v1.1

# Pipeline references by name@version — resolved from catalog
zinc flow processor swap enrich --to enrich_order@2.1
```

Pipeline definition in prod references catalog entries, not imports:

```zinc
var group = flow.Group("main")
group.addProcessor("validate", "validate_order@1.0")   // resolved from catalog
group.addProcessor("enrich", "enrich_order@2.1")        // swappable without redeploy
```

#### Hot-Swap Mechanics

Hot-swap mechanics differ by deployment mode:

**JVM mode (dev/staging)** — classloader-based swap within the same process. Stop worker virtual thread → load new class via isolated classloader → start worker virtual thread. The queue bridges the gap — items accumulate during the swap, new version picks them up. Fast iteration, no process restart.

**Native-image mode (prod/K8s)** — queue-bridged rolling restart. GraalVM native-image uses a closed-world assumption (all classes known at build time), so classloader tricks don't work. Instead, swap triggers a rolling Deployment update: old pod drains, new pod (with new processor binary) starts. NATS buffers messages during the ~100ms gap. From the operator's perspective the `zinc flow processor swap` command is identical — only the underlying mechanism changes.

This is actually simpler and more reliable in production than classloader hot-swap. It's the same pattern K8s rolling updates use, and it means the native-image binary is fully AOT-compiled with no reflection overhead.

```bash
# Swap in production — zero downtime
zinc flow processor swap enrich --to enrich_order@2.2 --reason "fixing timezone bug"
```

#### Versioned Flow State + Audit Trail + Rollback

Every change to the running flow (swap, scale, config, rewire) is a new revision in the state store. Full audit trail with instant rollback.

```bash
# Audit trail — every change is recorded
zinc flow history
  Rev 49  2026-03-19 14:23  vrjoshi  swap enrich -> enrich_order@2.2 "fixing timezone bug"
  Rev 48  2026-03-19 10:15  ops-bot  scale enrich-group replicas=10
  Rev 47  2026-03-18 09:00  deploy   full deploy from git@abc123
  Rev 46  2026-03-17 16:45  vrjoshi  rewire: added dead_letter after validate

# Diff between revisions
zinc flow diff 47 49

# Rollback — instant, one command
zinc flow rollback                   # revert to previous revision
zinc flow rollback --to 47           # revert to specific revision

# Detect drift from git
zinc flow drift
  enrich: catalog says enrich_order@2.2, git says enrich_order@2.1
  enrich-group: running 10 replicas, git says 3
```

This solves NiFi's Achilles heel: live graph changes are powerful but dangerous without auditability. Every change is tracked, diffable, and reversible. You get NiFi's speed of response with git-level safety.

#### Catalog Storage

The processor catalog stores:
- **Processor metadata**: name, version, description, input/output schemas, config schema
- **Processor code**: module path or package reference (the actual `.zn` files or compiled `.class` files)
- **Flow graph**: current processor wiring, group assignments, connection routes
- **Revision history**: every change with who/when/why/what

All stored in the state store (etcd or PostgreSQL). Phase 1 uses local filesystem for the catalog. Phase 2 adds the distributed state store.

### Q5: Schema enforcement — should FlowFile content have typed schemas?

**Answer: No. The framework does not enforce content schemas.**

FlowFile content is `byte[]`. The framework doesn't know or care what's inside — JSON, CSV, Avro, Parquet, binary, images, whatever. Content is opaque to the runtime.

Validation is the dataflow developer's responsibility. They add validation processors where needed:

```zinc
// Named outputs ["valid", "invalid"] declared in pipeline wiring
ProcessorFn validateJsonSchema = (ff) -> {
    var data = Json.parse(ff.content) or {
        return new Routed("invalid", ff.withAttribute("error", "not valid JSON"))
    }
    if data.get("id") == null or data.get("type") == null {
        return new Routed("invalid", ff.withAttribute("error", "missing required fields"))
    }
    return new Routed("valid", ff)
}
```

This is the right level of abstraction — the developer knows their data, the framework doesn't. NiFi works the same way.

### Q6: Multi-tenancy — multiple pipelines sharing infrastructure?

**Answer: Multiple pipelines, shared NATS, namespace isolation.**

Each pipeline has a name. Stream/subject names are namespaced: `zinc-flow.{pipeline}.{group}.{connection}`. Multiple pipelines can share the same NATS server without conflict.

No multi-tenant auth/isolation in Phase 1. If you need it, run separate NATS servers or use NATS accounts/auth.

### Q7: Expression language for routing?

**Answer: Two tiers — predicate-based rules for operators, compiled Zinc code for complex logic. No runtime eval.**

NiFi needs SpEL because processors are configured via XML/UI. SpEL is terrible — no validation, no autocomplete, cryptic errors, impossible to debug. But runtime `eval()` of any kind (SpEL, MVEL, Zinc expressions) is incompatible with GraalVM native-image's closed-world assumption.

Zinc Flow uses a two-tier approach inspired by the IRS backbone service:

#### Tier 1: Attribute-based predicates (80% of routing)

Routing rules are **data, not code** — serializable JSON, storable in state store, editable via CLI/API/UI. Evaluated by the `Router` using simple attribute lookups and enum-based operators. No reflection, no eval, fully AOT-compatible.

```zinc
// Predicate: attribute + operator + value
RoutingRule("high-priority",
    [RoutingPredicate("priority", RoutingOperator.EQ, "high")],
    RuleJoiner.AND,
    "zinc-flow.orders.enrich.high",
    10)
```

Operators: `EQ`, `NEQ`, `LT`, `GT`, `GTEQ`, `LTEQ`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `EXISTS`. Rules can be combined with `AND`/`OR` joiners. Covers the vast majority of routing decisions.

#### Tier 2: Compiled Zinc processors (20% — complex logic)

For routing that needs database lookups, multi-step decisions, or arithmetic — use a processor function. It's compiled at build time, AOT-safe, and can do anything. Named outputs are declared in the pipeline wiring:

```zinc
// Named outputs ["high", "normal", "low"] declared in pipeline wiring
ProcessorFn routeByPriority = (ff) -> {
    var priority = ff.attributes.getOrDefault("priority", "normal")
    return new Routed(priority, ff)
}
```

#### Low-code UI

The UI maps directly to predicates — each form row is a `RoutingPredicate`:

```
Route where:  [attribute ▼] [filename]  [contains ▼]  [.csv]   → route "csv_path"
              [attribute ▼] [priority]  [equals ▼]    [high]   → route "urgent"
              [otherwise]                               → route "default"
```

No expression parsing needed. The form serializes directly to `RoutingRule` JSON. Validation is trivial — the operator enum constrains what's valid.

#### Why this is better than SpEL / MVEL / eval

- **GraalVM native-image compatible** — no reflection, no dynamic class loading, no expression parsing
- **Serializable** — rules are JSON data, storable in state store, versionable, diffable
- **Simple to implement** — the `Router` is ~50 lines of attribute lookups, not a parser
- **Predictable performance** — O(rules × predicates) string comparisons, no compilation overhead
- **Complex logic stays compiled** — `ProcessorFn` implementations are AOT-compiled, type-checked, testable
- **IRS-proven** — this predicate model ran in production at enterprise scale

### Q8: Monitoring — Prometheus? OpenTelemetry?

**Answer: Prometheus metrics export via Micrometer. Phase 2.**

- Phase 1: Stats printed to terminal (msgs/sec, queue depth, errors)
- Phase 2: `/metrics` endpoint in Prometheus exposition format via Micrometer. Standard counters/gauges:
  - `zinc_flow_processed_total{processor="name"}` — counter
  - `zinc_flow_errors_total{processor="name"}` — counter
  - `zinc_flow_queue_depth{queue="name"}` — gauge
  - `zinc_flow_processing_seconds{processor="name"}` — histogram
- This plugs into existing Grafana/alerting stacks with zero custom tooling.

OpenTelemetry tracing (trace a FlowFile through the pipeline) is Phase 3 — nice to have, not critical for MVP.

### Q9: State management — counters, windows, dedup?

**Answer: External state stores. Processors are stateless by default.**

Stateful processors (dedup, windowed aggregation, counters) read/write state to an external store:

```zinc
class DedupProcessor : ProcessorFn {
    init StateStore stateStore

    init(StateStore stateStore) {
        this.stateStore = stateStore
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var key = ff.attributes.get("dedup.key")
        var seen = stateStore.get("dedup:seen:{key}")
        if seen != null {
            return new Drop()  // drop duplicate
        }
        stateStore.put("dedup:seen:{key}", "1")
        return new Single(ff)
    }
}
```

The processor is still stateless from the runtime's perspective — it can be restarted, scaled, or swapped without losing state. State lives in the external state store (etcd, PostgreSQL, Redis), not in the processor thread.

This is the same pattern Flink uses (state backends) and it's what makes horizontal scaling work — any replica of the processor can access the shared state.

### Q10: Ordering guarantees — FIFO per key? Per partition?

**Answer: Best-effort FIFO within a group. Keyed ordering between groups.**

- **Within a group (single replica)**: FIFO guaranteed — `Channel<T>` is FIFO, single consumer thread.
- **Within a group (multiple replicas)**: best-effort — multiple virtual threads compete for items. Order is not guaranteed across threads.
- **Between groups**: NATS JetStream is FIFO within a stream. For keyed ordering, use subject-based routing (e.g., `orders.{region}`) so related messages go to the same subject and are consumed in order.

For most data pipeline workloads, best-effort ordering is fine. If a processor needs strict ordering (e.g., CDC events), run it with 1 replica or use keyed partitioning.

---

## Project Structure

```
zinc-flow/
    flow/
        init.zn              # ProcessorFn interface, Pipeline, Group
        flowfile.zn          # FlowFile data class
        pipeline.zn          # Pipeline, ProcessorGroup, Connection
        worker.zn            # ProcessorWorker, run loop
        queue.zn             # FlowQueue interface, LocalQueue
        queue_nats.zn        # NatsQueue — NATS JetStream (Phase 2)
        router.zn            # Router interface, routing rules
        content_store.zn     # ContentStore interface, FileContentStore
        state_store.zn       # StateStore interface (Phase 2)
        services.zn          # ServiceRegistry, SecretsProvider
        stats.zn             # ProcessorStats, throughput tracking
        serialization.zn     # FlowFile serialization for cross-group transport
        test/
            init.zn          # FlowTest helpers, assertions
            harness.zn       # PipelineHarness — in-memory test pipeline
            mocks.zn         # MockSource, MockSink, MockServiceRegistry, MockSecretsProvider
        sources/
            http.zn          # HTTP source (Javalin)
            kafka.zn         # Kafka consumer source
            filesystem.zn    # Directory watcher source
        sinks/
            http.zn          # HTTP POST sink
            kafka.zn         # Kafka producer sink
            filesystem.zn    # File writer sink
            s3.zn            # S3-compatible object writer sink
    cli/
        flow_cmd.zn          # zinc flow run/status/processor/queues/test
    tests/
        test_flowfile.zn     # Phase 1a
        test_processor.zn    # Phase 1b
        test_queue.zn        # Phase 1c
        test_worker.zn       # Phase 1d
        test_group.zn        # Phase 1e
        test_pipeline.zn     # Phase 1f
        test_source_sink.zn  # Phase 1g
        test_e2e.zn          # Phase 1h
        test_performance.zn  # Phase 1j
        test_routing.zn      # Phase 2f
        test_secrets.zn      # Phase 2h
```

---

## Testing Strategy

Testing grows with the system — each vertical gets tests as it's built. No big bang test phase.

### Testing Levels

| Level | What | How | When |
|-------|------|-----|------|
| **Unit** | Single processor function | Mock FlowFile in, assert FlowFile out | Every processor |
| **Queue** | Queue behavior | Put/get, backpressure, ordering, thread safety | When queue is built |
| **Routing** | Routing rules | Assert FlowFile matches correct route | When routing is built |
| **Integration** | Mini pipeline end-to-end | Spin up in-memory pipeline, push FlowFiles, assert output | When pipeline wiring works |
| **Performance** | Throughput/latency regression | Benchmarks with known baselines | Before each release |

### Test Harness — `zinc flow test`

Built-in test runner that understands FlowFile pipelines:

```zinc
import flow.test

// Unit test — test a processor in isolation
@FlowTest
fn test_parse_json() {
    var input = FlowFile(
        "test-1",
        {"source": "test"},
        "{\"type\": \"order\", \"id\": 123}".getBytes(),
        0L
    )

    var output = parse_json(input)

    assert output.attributes.get("record_type") == "order"
    assert Json.parse(output.content).containsKey("id")
}

// Unit test — test routing
@FlowTest
fn test_validate_routes_invalid() {
    var input = FlowFile(
        "test-2",
        {},
        "{\"no_id\": true}".getBytes(),
        0L
    )

    var (route, output) = validate(input)

    assert route == "invalid"
    assert output.attributes.get("error") == "missing required fields"
}

// Integration test — test a mini pipeline
@FlowTest
fn test_ingest_pipeline() {
    var harness = flow.test.PipelineHarness()

    // Build a test pipeline with in-memory source/sink
    var source = flow.test.MockSource()
    var sink = flow.test.MockSink()

    harness.addSource(source)
    harness.addProcessor("parse", parse_json)
    harness.addProcessor("validate", validate)
    harness.addSink("output", sink, route = "valid")
    harness.connect("source", "parse")
    harness.connect("parse", "validate")
    harness.connect("validate", "output", route = "valid")

    // Push test data and run
    source.send(FlowFile(
        "test-3",
        {},
        "{\"id\": 1, \"type\": \"order\"}".getBytes(),
        0L
    ))

    harness.runUntilIdle(timeout = 5000L)

    // Assert what came out
    assert sink.received.size() == 1
    assert sink.received.get(0).attributes.get("record_type") == "order"
}

// Queue test — verify backpressure
@FlowTest
fn test_queue_backpressure() {
    var q = LocalQueue(maxSize = 5)

    // Fill the queue
    for i in 0..5 {
        q.put(FlowFile("ff-{i}", {}, "x".getBytes(), 0L))
    }

    assert q.size() == 5

    // Next put should block (or timeout via offer)
    var accepted = q.offer(FlowFile("ff-6", {}, "x".getBytes(), 0L), 100L)
    assert accepted == false  // queue full, rejected
}

// Routing rule test
@FlowTest
fn test_routing_rules() {
    var router = Router(rules = [
        RoutingRule(
            "high",
            [RoutingPredicate("priority", RoutingOperator.EQ, "high")],
            RuleJoiner.AND,
            "enrich.high",
            10
        ),
        RoutingRule(
            "default",
            [],
            RuleJoiner.AND,
            "enrich.default",
            0
        ),
    ])

    var highFf = FlowFile("1", {"priority": "high"}, "".getBytes(), 0L)
    var lowFf = FlowFile("2", {"priority": "low"}, "".getBytes(), 0L)

    assert router.route(highFf) == "enrich.high"
    assert router.route(lowFf) == "enrich.default"
}

// Performance test — verify throughput hasn't regressed
@FlowTest(performance = true)
fn test_queue_throughput() {
    var q = LocalQueue(maxSize = 10_000)
    var ff = FlowFile("perf", {}, Random().nextBytes(10 * 1024), 0L)

    var start = System.nanoTime()
    for i in 0..50_000 {
        q.put(ff)
        q.poll(0)
    }
    var elapsed = (System.nanoTime() - start) / 1_000_000_000.0
    var msgsPerSec = 50_000.0 / elapsed

    assert msgsPerSec > 50_000, "Queue throughput regression: {msgsPerSec} msgs/sec"
}
```

```bash
# Run all tests
zinc flow test

# Run unit tests only
zinc flow test --unit

# Run integration tests
zinc flow test --integration

# Run performance tests
zinc flow test --performance

# Run tests for a specific vertical
zinc flow test tests/test_processor.zn
zinc flow test tests/test_routing.zn
```

### Test Helpers

The `flow.test` module provides:

```zinc
class MockSource {
    // Programmatically inject FlowFiles into a pipeline
    pub fn send(FlowFile ff) { ... }
    pub fn sendBatch(List<FlowFile> ffs) { ... }
}

class MockSink {
    // Capture FlowFiles that exit a pipeline
    pub var List<FlowFile> received = []
    pub fn reset() { received.clear() }
}

class PipelineHarness {
    // Spin up an in-memory pipeline for testing
    // All queues are LocalQueue, no external dependencies
    pub fn addSource(MockSource source) { ... }
    pub fn addProcessor(String name, ProcessorFn fn) { ... }
    pub fn addSink(String name, MockSink sink, String route = "default") { ... }
    pub fn connect(String source, String target, String route = "default") { ... }
    pub fn runUntilIdle(long timeout = 5000L) { ... }
    pub fn runFor(long millis) { ... }
}

class MockServiceRegistry : ServiceRegistry {
    // Inject mock services for testing (mock DB, mock HTTP, etc.)
}

class MockSecretsProvider : SecretsProvider {
    // Return test secrets without Vault/env/file
    var Map<String, String> secrets = {}
    pub fn get(String key): String { return secrets.get(key) }
}
```

All mocks implement the same interfaces as production components (design-by-interface pays off here).

---

## Implementation Phases

Each phase is broken into verticals. Tests are built alongside each vertical — not after.

### Phase 1 — MVP (Local Dev)

| Vertical | Build | Test |
|----------|-------|------|
| **1a. FlowFile** | `data FlowFile` with `withContent()`, `withAttribute()` | Unit: create, transform, immutability |
| **1b. Processor** | `ProcessorFn` interface, return types (Single, Multiple, Drop, Routed) | Unit: processor in/out, all return types, error handling |
| **1c. Queue** | `FlowQueue` interface, `LocalQueue` implementation | Unit: put/get, thread safety, backpressure, ordering |
| **1d. Worker** | `ProcessorWorker` — virtual thread-based consumer loop, retry, DLQ | Unit: consume from queue, route output, retry logic, DLQ |
| **1e. Group** | `ProcessorGroup` — start/stop/scale workers | Unit: lifecycle, scaling virtual threads |
| **1f. Pipeline** | `Pipeline` — connect groups, run all workers in local mode | Integration: multi-processor pipeline end-to-end |
| **1g. Source/Sink** | HTTP source, filesystem sink | Integration: POST a FlowFile, verify it reaches disk |
| **1h. CLI** | `zinc flow run pipeline.zn` | E2E: run a pipeline, POST data, check output files |
| **1i. Stats** | Terminal stats (msgs/sec, queue depth, errors) | Manual verification |
| **1j. Performance** | Throughput baselines | Performance: assert no regression |

**Not in Phase 1**: Pipeline DSL (`->` syntax), distributed queues, content store, management REST API, K8s deploy, hot-swap, Prometheus metrics.

### Phase 2 — Production Ready

| Vertical | Build | Test |
|----------|-------|------|
| **2a. Pipeline DSL** | `->` chaining and group definitions | Unit: parse DSL, verify wiring matches explicit API |
| **2b. NATS Queue** | `NatsQueue` implementation of `FlowQueue` | Integration: put/get through real NATS, consumer groups |
| **2c. Content Store** | `FileContentStore`, large FlowFile handoff | Integration: store/retrieve/delete, verify cleanup |
| **2d. State Store** | etcd/PostgreSQL `StateStore` implementation | Integration: CRUD, revision history, rollback |
| **2e. Processor Catalog** | Publish, discover, hot-swap processors | Integration: publish, swap, verify queue bridges gap |
| **2f. Routing** | `Router` with NATS subject-based routing, routing table in state store | Unit: rule evaluation. Integration: dynamic rule changes |
| **2g. REST API** | Management API (start/stop/scale/swap/config) | Integration: API calls, verify pipeline state changes |
| **2h. Secrets** | `SecretsProvider` chain (env, file, Vault) | Unit: resolution, fallback chain |
| **2i. Observability** | Prometheus `/metrics` via Micrometer, structured logging | Integration: verify metrics emitted, log format |
| **2j. Back-pressure** | NATS stream limits, cross-group backpressure | Integration: fill queue, verify upstream slows |
| **2k. Docker Compose** | `zinc flow deploy` generates Compose for multi-group | E2E: deploy, send data, verify cross-group routing |

### Phase 3 — Cloud Native

| Vertical | Build | Test |
|----------|-------|------|
| **3a. K8s Operator** | `zinc flow deploy` generates Deployments per group | E2E: deploy to K8s, verify pods, cross-group NATS |
| **3b. Auto-scaling** | HPA with NATS consumer lag metrics | Load: sustained traffic, verify scale-up/down |
| **3c. Kafka Backend** | `KafkaQueue` pluggable alternative | Integration: same tests as NatsQueue, different backend |
| **3d. Tracing** | OpenTelemetry, trace FlowFile across groups | Integration: verify trace context propagation |
| **3e. TUI** | Terminal dashboard via REST API | Manual verification |

### Phase 4 — Enterprise

| Vertical | Build | Test |
|----------|-------|------|
| **4a. Provenance** | FlowFile lineage tracking and visualization | Integration: trace FlowFile history |
| **4b. RBAC** | Role-based access on management API | Unit: permission checks. Integration: API auth |
| **4c. Audit** | Audit logging for all management actions | Integration: verify log entries |
| **4d. Multi-pipeline** | Multiple pipelines on shared infrastructure | Integration: namespace isolation, no cross-talk |
| **4e. Web UI** | Low-code UI with Zinc expression validation | E2E: UI tests |

---

## Phase 1 End-to-End Example

```zinc
import flow

ProcessorFn parse_json = (ff) -> {
    var data = Json.parse(ff.content)
    return new Single(ff.withAttribute("record_type", data.getOrDefault("type", "unknown").toString())
              .withContent(Json.toPrettyBytes(data)))
}

ProcessorFn add_timestamp = (ff) -> {
    return new Single(ff.withAttribute("processed_at", Instant.now().toString()))
}

// Named outputs ["valid", "invalid"] declared in pipeline wiring below
ProcessorFn validate = (ff) -> {
    var data = Json.parse(ff.content)
    if data.containsKey("id") and data.containsKey("type") {
        return new Routed("valid", ff)
    }
    return new Routed("invalid", ff.withAttribute("error", "missing required fields"))
}

// Phase 1 — explicit wiring (no DSL yet)
var pipeline = flow.Pipeline("ingest")

var group = flow.Group("main")
group.addSource(flow.sources.http(port = 8080, path = "/data"))
group.addProcessor("parse", parse_json)
group.addProcessor("timestamp", add_timestamp)
group.addProcessor("validate", validate)
group.addSink("valid_out", flow.sinks.filesystem("/data/output/valid/"), route = "valid")
group.addSink("invalid_out", flow.sinks.filesystem("/data/output/invalid/"), route = "invalid")

group.connect("source", "parse")
group.connect("parse", "timestamp")
group.connect("timestamp", "validate")
group.connect("validate", "valid_out", route = "valid")
group.connect("validate", "invalid_out", route = "invalid")

pipeline.addGroup(group)
pipeline.run()
```

```bash
$ zinc flow run ingest.zn
Pipeline 'ingest' starting (local mode)...
  [main/http-source]   listening on :8080/data
  [main/parse]         running (1 virtual thread)
  [main/timestamp]     running (1 virtual thread)
  [main/validate]      running (1 virtual thread)
  [main/valid_out]     writing to /data/output/valid/
  [main/invalid_out]   writing to /data/output/invalid/

Pipeline running. Ctrl+C to stop.

Stats (every 5s):
  main/parse:      1,247 msgs/s | queue: 12/10000 | errors: 0
  main/timestamp:  1,245 msgs/s | queue:  3/10000 | errors: 0
  main/validate:   1,243 msgs/s | queue:  5/10000 | errors: 2
```
