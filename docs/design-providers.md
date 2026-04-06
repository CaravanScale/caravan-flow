# Flow Engine Design — Providers, Queues, Transactions, and Lifecycle

## Context

zinc-flow needs a reliable flow engine with transactional delivery guarantees, dynamic lifecycle management, and backpressure. The design draws from NiFi's session model and the IRS predicate routing layer. The core guarantee: **a flowfile is never lost** — it is only removed from its source queue after confirmed delivery to all destinations.

## Design Principles

1. **At-least-once delivery** — flowfiles are never removed until confirmed downstream
2. **All-or-nothing fan-out** — IRS pre-checks all destinations before committing
3. **Fail-fast startup** — missing/broken provider = refuse to start
4. **Feature flags everywhere** — providers and processors can be enabled/disabled at runtime
5. **Dependency-aware lifecycle** — disable a provider, its dependents drain and disable
6. **Backpressure propagates backwards** — full queues block upstream naturally
7. **No infinite anything** — retries, timeouts, and queue sizes are all bounded
8. **Scoped access** — processors only see providers they declare as dependencies

---

## 1. Transactional Queue Model

The queue is the fundamental unit connecting processors. Every queue item has one of three states:

```
VISIBLE ──claim()──→ INVISIBLE ──ack()──→ removed
                         │
                         ├──nack()──→ VISIBLE (immediate)
                         └──timeout──→ VISIBLE (automatic)
```

- **Visible**: available for a processor to claim
- **Invisible**: claimed by a processor, with a visibility timeout
- **Acked**: confirmed processed, permanently removed from queue

If a processor crashes or hangs, the visibility timeout expires and the item becomes visible again — automatic recovery with no data loss.

### FlowQueue

```zinc
class FlowQueue {
    String name
    int maxCount                        // backpressure limit: item count
    long maxBytes                       // backpressure limit: total bytes
    Duration visibilityTimeout          // how long a claimed item stays invisible

    pub fn offer(FlowFile ff): bool     // enqueue, false if full (backpressure)
    pub fn claim(): QueueEntry?         // claim next visible item, null if empty
    pub fn ack(String entryId)          // confirmed — remove permanently
    pub fn nack(String entryId)         // failed — make visible immediately
    pub fn hasCapacity(): bool          // pre-check for IRS fan-out
    pub fn size(): int                  // current item count
    pub fn bytes(): long                // current total bytes
}
```

### QueueEntry

```zinc
class QueueEntry {
    String id
    FlowFile flowFile
    Instant claimedAt
    int attemptCount                    // how many times this item has been claimed
    String sourceProcessor              // where it came from (for DLQ context)
}
```

---

## 2. Provider Model

### Provider Interface

Providers have lifecycle states and can be enabled/disabled at runtime.

```zinc
interface Provider {
    fn getName(): String
    fn getType(): String
    fn getState(): ComponentState       // enabled | draining | disabled
    fn enable()
    fn disable(Duration drainTimeout)
    fn close()                          // shutdown — release resources
}
```

### ComponentState

```zinc
enum ComponentState {
    DISABLED,
    ENABLED,
    DRAINING
}
```

### Concrete Providers (Phase 1)

**ConfigProvider** — wraps `Config`, the bootstrap provider used to configure everything else.

```zinc
class ConfigProvider : Provider {
    String name = "config"
    ComponentState state
    Config cfg

    pub fn getName(): String { return name }
    pub fn getType(): String { return "config" }
    pub fn getState(): ComponentState { return state }
    pub fn enable() { state = ComponentState.ENABLED }
    pub fn disable(Duration timeout) { state = ComponentState.DISABLED }
    pub fn close() {}

    pub fn getString(String key): String
    pub fn getInt(String key): int
    pub fn getBool(String key): bool
    pub fn has(String key): bool
    pub fn getStringMap(String key): Map<String, String>
    pub fn getProcessorConfig(String processorName): Map<String, String> {
        return cfg.getStringMap("flow.processors.${processorName}.config")
    }
}
```

**LoggingProvider** — wraps `LogManager`.

```zinc
class LoggingProvider : Provider {
    String name = "logging"
    ComponentState state
    LogManager lm

    pub fn getName(): String { return name }
    pub fn getType(): String { return "logging" }
    pub fn getState(): ComponentState { return state }
    pub fn enable() { state = ComponentState.ENABLED }
    pub fn disable(Duration timeout) { state = ComponentState.DISABLED }
    pub fn close() {}

    pub fn getLogManager(): LogManager { return lm }
}
```

**ContentProvider** — wraps `ContentStore` (FileContentStore or MemoryContentStore).

```zinc
class ContentProvider : Provider {
    String name
    ComponentState state
    ContentStore store

    pub fn getName(): String { return name }
    pub fn getType(): String { return "content" }
    pub fn getState(): ComponentState { return state }
    pub fn enable() { state = ComponentState.ENABLED }
    pub fn disable(Duration timeout) { state = ComponentState.DISABLED }
    pub fn close() {}

    pub fn getStore(): ContentStore { return store }
}
```

---

## 3. Scoped Context

Processors declare their provider dependencies via `requires` in YAML. At construction time, they receive a `ScopedContext` containing only the providers they declared — no ambient access to everything.

```zinc
class ScopedContext {
    Map<String, Provider> allowed

    pub fn getProvider(String name): Provider {
        if !allowed.has(name) {
            return Error("provider '${name}' not in scope for this processor")
        }
        return allowed[name]
    }

    pub fn listProviders(): List<String> {
        return allowed.keys()
    }
}
```

**Construction**: the Fabric builds a ScopedContext per processor from its `requires` list:

```zinc
fn buildScopedContext(List<String> requires, ProcessorContext global): ScopedContext {
    var allowed = Map<String, Provider>{}
    for name in requires {
        var provider = global.getProvider(name)
        allowed[name] = provider
    }
    return ScopedContext { allowed = allowed }
}
```

---

## 4. Component Lifecycle

### States

```
DISABLED ──enable()──→ ENABLED ──disable()──→ DRAINING ──(complete | timeout)──→ DISABLED
```

- **Disabled**: not processing, not accepting flowfiles
- **Enabled**: actively processing
- **Draining**: no new claims from input queue, in-flight items complete their claim/process/ack cycle. On drain timeout, un-acked items return to visible in source queue (no data loss).

### Feature Flags

- **YAML** defines the initial enabled/disabled state at startup
- **API** can toggle at runtime (`POST /api/processors/{name}/enable`, `POST /api/providers/{name}/disable`)
- Runtime state lives in memory — does not mutate the YAML file
- On restart, YAML is truth

### Dependency-Aware Cascading

Providers and processors form a dependency DAG:

```
provider:content ←── requires ── processor:file-sink
provider:content ←── requires ── processor:file-source
provider:logging ←── (implicit) ── all processors
```

**Disable provider** → all dependent processors enter `DRAINING` → then `DISABLED`.

**Enable processor** → check all required providers are `ENABLED`. Refuse if any are `DISABLED` or `DRAINING`.

### Drain Behavior

1. Processor stops claiming new items from its input queue
2. In-flight items (already claimed/invisible) continue processing
3. If processing completes within the drain timeout:
   - Ack the source queue entry (normal completion)
   - Processor transitions to `DISABLED`
4. If drain timeout expires:
   - Un-acked items return to `VISIBLE` in the source queue (visibility timeout)
   - Processor transitions to `DISABLED`
   - No data loss — items will be reprocessed when re-enabled

---

## 5. IRS Integration (Routing and Fan-Out)

The IRS is the **only fan-out point** in the flow. Processors are always 1-in, 1-out. After a processor emits its result, the IRS evaluates predicate routing rules and may deliver to multiple destination queues.

### Flow Sequence

```
1. Processor claims FlowFile from its input queue → item goes INVISIBLE
2. Processor processes it → emits one result FlowFile
3. IRS evaluates routing rules against result → matched destinations: [A, B, C]
4. Pre-check: do ALL destination queues have capacity?
   ├── Any full → nack source, retry later (item goes VISIBLE in source queue)
   │   └── Retry count exceeded → send to DLQ
   └── All have capacity → offer to A, B, C (confirmed)
5. ACK source → permanently removed
```

**All-or-nothing**: the IRS never offers to any destination until it confirms all destinations can accept. No partial commits, no rollback needed.

### ProcessSession

The transaction boundary wrapping one processor execution cycle:

```zinc
class ProcessSession {
    FlowQueue source
    ProcessorFn processor
    RoutingEngine irs
    FlowQueue dlq
    int maxRetries

    pub fn execute() {
        var entry = source.claim()
        if entry == null { return }

        // Process
        var result = processor.process(entry.flowFile)

        // Route
        var destinations = irs.evaluate(result)

        // Pre-check all destinations
        for dest in destinations {
            if !dest.hasCapacity() {
                source.nack(entry.id)           // backpressure — retry later
                return
            }
        }

        // Check retry limit
        if entry.attemptCount >= maxRetries {
            dlq.offer(entry.flowFile)           // exceeded retries → DLQ
            source.ack(entry.id)                // remove from source (it's in DLQ now)
            return
        }

        // All-or-nothing commit
        for dest in destinations {
            dest.offer(result)                  // confirmed — pre-check passed
        }
        source.ack(entry.id)                    // transaction complete
    }
}
```

---

## 6. Backpressure

### Mechanism

Each processor's input queue has bounded capacity (`maxCount` and `maxBytes`). When a queue is full:

1. Upstream IRS pre-check detects the full queue
2. IRS nacks the source item → stays visible in upstream queue
3. Upstream processor can't complete its transaction → it effectively pauses
4. Backpressure propagates backwards through the flow graph naturally

### Configuration

zinc-flow defines defaults. YAML can override per-processor.

```zinc
// zinc-flow defaults
const DEFAULT_QUEUE_MAX_COUNT = 10000
const DEFAULT_QUEUE_MAX_BYTES = 100 * 1024 * 1024   // 100 MB
const DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30)
const DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(60)
const DEFAULT_MAX_RETRIES = 5
```

---

## 7. Dead Letter Queue (DLQ)

One DLQ per flow. Items land here when they exceed their retry limit.

### DLQ Entry

```zinc
class DLQEntry {
    FlowFile flowFile
    String sourceProcessor              // which processor last attempted it
    String sourceQueue                  // which queue it came from
    int attemptCount                    // total attempts before giving up
    String lastError                    // why the last attempt failed
    Instant arrivedAt                   // when it entered the DLQ
}
```

### DLQ API

```
GET  /api/dlq                          // list DLQ entries (inspectable)
GET  /api/dlq/{id}                     // get specific entry with details
POST /api/dlq/{id}/replay              // re-inject into the appropriate source queue
POST /api/dlq/replay-all               // replay everything in the DLQ
DELETE /api/dlq/{id}                   // discard entry
```

Replay inserts the flowfile back into the queue it originally came from (`sourceQueue`), with the attempt count reset.

---

## 8. ProcessorContext (Global)

The global context holds all providers. The Fabric uses it to build ScopedContexts per processor.

```zinc
class ProcessorContext {
    var providers = Map<String, Provider>{}

    pub fn addProvider(Provider provider)
    pub fn removeProvider(String name)
    pub fn getProvider(String name): Provider
    pub fn listProviders(): List<String>
    pub fn disableProvider(String name, Duration drainTimeout)   // cascade to dependents
    pub fn enableProvider(String name)
    pub fn closeAll()
}
```

---

## 9. ProcessorFactory Signature

```zinc
type ProcessorFactory = Fn<(ScopedContext, Map<String, String>), ProcessorFn>
```

Factories receive only the scoped providers they declared:

```zinc
fn FileSinkFactory(ScopedContext ctx, Map<String, String> config): ProcessorFn {
    var cp = ctx.getProvider("content") as ContentProvider
    return FileSink(config["output_dir"], cp.getStore())
}
```

---

## 10. YAML Config Structure

```yaml
server:
  port: 9091

logging:
  level: DEBUG
  handler: text
  output: stdout

defaults:
  backpressure:
    max_count: 10000
    max_bytes: 104857600          # 100 MB
    visibility_timeout: 30s
    drain_timeout: 60s
    max_retries: 5

providers:
  content:
    type: file-content
    enabled: true
    config:
      dir: /tmp/zinc-flow/content

flow:
  processors:
    tag-env:
      type: add-attribute
      enabled: true
      config:
        key: env
        value: dev

    tag-source:
      type: add-attribute
      enabled: true
      config:
        key: source
        value: api

    logger:
      type: log
      enabled: true
      config:
        prefix: flow

    sink:
      type: file-sink
      enabled: true
      requires: [content]
      backpressure:
        max_count: 5000           # override default for this processor
      config:
        output_dir: /tmp/zinc-flow/output

  routes:
    tag-all:
      condition:
        attribute: type
        operator: EXISTS
      destination: tag-env

    after-tag-env:
      condition:
        attribute: env
        operator: EQ
        value: dev
      destination: tag-source

    after-tag-source:
      condition:
        attribute: source
        operator: EXISTS
      destination: logger

    after-log:
      condition:
        attribute: source
        operator: EXISTS
      destination: sink
```

---

## 11. Bootstrap Sequence

```
1.  Load Config from YAML/env
2.  Create ConfigProvider wrapping Config — first provider, always enabled
3.  Create LoggingProvider wrapping LogManager — always enabled
4.  Read providers section from config, instantiate each by type:
    - Check enabled flag — skip disabled providers
    - type: file-content → FileContentStore wrapped in ContentProvider
    - type: memory-content → MemoryContentStore wrapped in ContentProvider
5.  Add all providers to ProcessorContext
6.  Validate provider dependencies — fail fast if a required provider is missing
7.  Create Registry, register builtins
8.  Create FlowQueues for each processor (using defaults + per-processor overrides)
9.  Create global DLQ
10. Create Fabric with Registry + ProcessorContext
11. Fabric.loadFlow():
    - Read map-keyed processors from ConfigProvider
    - Check enabled flag — skip disabled processors
    - Validate requires — all required providers must exist and be enabled
    - Build ScopedContext per processor from requires
    - Create ProcessorFn via factory with ScopedContext + config
    - Read map-keyed routes, build IRS routing rules
    - Wire ProcessSessions: source queue → processor → IRS → destination queues
12. Start HTTP server (ingest endpoint feeds the first queue)
```

---

## 12. API Endpoints (Lifecycle Management)

```
POST   /api/providers/{name}/enable
POST   /api/providers/{name}/disable
GET    /api/providers/{name}/state

POST   /api/processors/{name}/enable
POST   /api/processors/{name}/disable
GET    /api/processors/{name}/state

GET    /api/queues                      // all queue depths and capacity
GET    /api/queues/{name}               // specific queue stats

GET    /api/dlq
GET    /api/dlq/{id}
POST   /api/dlq/{id}/replay
POST   /api/dlq/replay-all
DELETE /api/dlq/{id}
```

---

## 13. Files to Modify

| File | Change |
|------|--------|
| `src/core/context.zn` | Provider interface with lifecycle (enable/disable/getState). ProcessorContext with cascade disable. ComponentState enum |
| `src/core/queue.zn` | **New** — FlowQueue, QueueEntry, visibility timeout reaper |
| `src/core/session.zn` | **New** — ProcessSession, transaction logic, retry + DLQ routing |
| `src/core/scoped_context.zn` | **New** — ScopedContext for per-processor provider access |
| `src/core/providers.zn` | **New** — ConfigProvider, LoggingProvider, ContentProvider with lifecycle |
| `src/core/dlq.zn` | **New** — DLQ storage, DLQEntry, replay logic |
| `src/fabric/registry/registry.zn` | ProcessorFactory type: `Fn<(ScopedContext, Map<String, String>), ProcessorFn>` |
| `src/fabric/runtime/runtime.zn` | Replace ServiceProvider with ProcessorContext. Wire ProcessSessions. Manage queues and lifecycle |
| `src/processors/builtin.zn` | Update all factory signatures to `(ScopedContext, Map<String, String>)` |
| `src/main.zn` | New bootstrap sequence. Provider instantiation, queue creation, DLQ setup |
| `src/http/api.zn` | Lifecycle API endpoints, queue stats, DLQ endpoints |
| `config.yaml` | Add defaults section, enabled flags, requires declarations |

---

## 14. Verification

1. **Startup** — `zinc build && zinc run` with updated config. All providers and processors initialize, queues created, DLQ ready
2. **Ingest** — `POST /ingest` a FlowFile. Routes through all enabled processors, arrives at sink
3. **Backpressure** — flood ingest, verify queue depths cap at configured limits, upstream pauses
4. **Disable provider** — `POST /api/providers/content/disable`. Verify dependent processors (sink) drain and disable
5. **Enable check** — `POST /api/processors/sink/enable` while content provider is disabled. Should refuse
6. **Re-enable** — enable content provider, then sink. Verify processing resumes
7. **DLQ** — configure a processor to fail, verify items land in DLQ after max retries. Replay via API, verify reprocessing
8. **Drain** — disable a processor mid-flow, verify in-flight items complete, no data loss
9. **Shutdown** — kill process, verify `closeAll()` fires, un-acked items are recoverable on restart
