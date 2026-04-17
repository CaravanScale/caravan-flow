# caravan-flow Architecture

Lightweight data flow engine inspired by Apache NiFi (processor model), Apache Camel (direct pipeline execution), and NiFi Stateless (ephemeral, no inter-stage queues).

---

## Design Principles

1. **Direct execution, no queues.** FlowFiles flow synchronously through the processor DAG on the calling thread. No inter-stage queues, no claim/ack, no visibility timeouts. Concurrency comes from sources, not from parallelism between stages.

2. **Configuration-driven.** Processors are pre-built components. The flow graph is YAML config — processors, connections, and source connectors. No code to write for common pipelines.

3. **Failure routing in-graph.** Processors return typed results (success, failure, drop). Failures follow "failure" connections to error-handling processors. No external DLQ — error handling is part of the graph.

4. **Scale by decomposition.** A single caravan-flow process handles one flow graph. For scaling, break the graph into fast/slow parts connected via NATS between separate caravan-flow instances (pods). No distributed coordination within a single process.

5. **Zero external deps.** The standalone binary has no runtime dependencies — no database, no message broker, no JVM. Config from YAML, content on disk.

---

## Execution Model

```
Source (ListenHTTP / GetFile)
  │
  │  Each source thread calls IngestAndExecute(FlowFile)
  │  concurrently — ASP.NET Kestrel handles HTTP threading,
  │  GetFile spawns tasks per file.
  ▼
┌─────────────────────────────────────────────────────┐
│  Fabric.Execute(ff, entryPoint)                     │
│                                                     │
│  SemaphoreSlim gate (max_concurrent_executions)     │
│                                                     │
│  Iterative work-stack loop:                         │
│    while work.Count > 0:                            │
│      pop (FlowFile, processorName, hops)            │
│      hop limit check (default 50)                   │
│      result = processor.Process(ff)                 │
│      match result:                                  │
│        SingleResult   → push "success" targets      │
│        MultipleResult → push each to "success"      │
│        RoutedResult   → push named relationship     │
│        DroppedResult  → return ff to pool            │
│        FailureResult  → push "failure" targets       │
│                         or log+drop if none          │
│      fan-out: first target gets original,            │
│               rest get clones (Content.AddRef)       │
│                                                     │
│  Depth-first: each branch completes before next     │
└─────────────────────────────────────────────────────┘
```

Each `Execute()` call is synchronous and independent. Many FlowFiles execute concurrently across different threads, but each individual FlowFile traversal is sequential. This is the same model as ASP.NET middleware, Apache Camel routes, and NiFi Stateless.

---

## Core Types

### FlowFile
```
FlowFile
├── NumericId: long          — unique ID (pool-assigned)
├── Attributes: AttributeMap — immutable overlay chain (key-value metadata)
├── Content: Content         — sealed type (Raw | RecordContent | ClaimContent)
├── Timestamp: long          — creation time (TickCount64)
└── HopCount: int            — cycle protection counter
```
- **Object-pooled** via thread-local `Pool<T>` (256 per thread, no CAS contention)
- **AttributeMap** uses immutable overlay chain — `With(key, value)` creates a new layer pointing to parent, avoiding dict copies
- **Content** is reference-counted — `AddRef()` / `Release()` for safe sharing during fan-out

### Content (sealed hierarchy)
```
Content
├── Raw           — ArrayPool-backed byte array (inline, small data)
├── RecordContent — Schema + List<GenericRecord> (Avro-style typed records)
└── ClaimContent  — off-heap reference (content store claim ID, for >256KB)
```

### ProcessorResult (sealed hierarchy)
```
ProcessorResult
├── SingleResult   — one FlowFile out (most common)
├── MultipleResult — split into N FlowFiles (e.g., SplitText)
├── RoutedResult   — one FlowFile to a named relationship
├── DroppedResult  — discard (filter/dedup)
└── FailureResult  — error with reason string
```
All result types are **object-pooled** with `Rent()` / `Return()`.

### IProcessor
```csharp
public interface IProcessor
{
    ProcessorResult Process(FlowFile ff);
}
```
Stateless, pure function. Takes a FlowFile, returns a result. No side-channel access — only what's in the FlowFile and the processor's config.

---

## Pipeline Graph

```yaml
flow:
  processors:
    parse-json:
      type: ConvertJSONToRecord
      connections:
        success:
          - enrich
        failure:
          - error-handler

    enrich:
      type: UpdateAttribute
      config:
        env: prod
      connections:
        success:
          - sink

    sink:
      type: PutFile
      requires:
        - content
      config:
        output_dir: /tmp/output

    error-handler:
      type: LogAttribute
      config:
        prefix: ERROR
```

- **Connections** are per-processor, keyed by relationship name (`success`, `failure`, custom)
- **Entry points** are processors not referenced by any connection — they receive ingested FlowFiles
- **Sinks** are processors with no outgoing connections — FlowFiles terminate there
- **DAG validator** runs at load time: cycle detection (DFS 3-color), unreachable processor detection, entry-point computation

### PipelineGraph (internal)

Immutable structure, swapped atomically on hot reload via `volatile` write:
```
PipelineGraph
├── Processors: Dictionary<name, IProcessor>
├── Connections: Dictionary<name, Dictionary<relationship, List<target>>>
├── EntryPoints: List<name>
├── ProcessorNames: List<name>
├── ProcessorStates: Dictionary<name, ComponentState>
└── ProcessorDefs: Dictionary<name, (Type, Config, Requires)>
```

---

## Providers

Shared infrastructure exposed to processors via `ScopedContext`:

| Provider | Purpose |
|----------|---------|
| **ContentProvider** | Content store for off-heap claims (FileContentStore / MemoryContentStore) |
| **ConfigProvider** | YAML config access with dot-path navigation |
| **LoggingProvider** | Structured (JSON) or text logging |
| **ProvenanceProvider** | Ring buffer of FlowFile lifecycle events |

Processors declare `requires: [content, logging]` in config. They receive a `ScopedContext` with only their declared dependencies — no global access.

---

## Source Connectors

All sources implement `IConnectorSource`. There are two patterns:

```
IConnectorSource (lifecycle interface)
├── Event-driven — implement directly, use library's native threading
│   ListenHTTP (Kestrel), GetNats (NATS client), GetKafka (consumer)
│
└── PollingSource (abstract base class) — implement Poll(), framework schedules
    GetFile, GetFTP, GetJDBC
```

### IConnectorSource (interface)
```csharp
public interface IConnectorSource
{
    string Name { get; }
    string SourceType { get; }
    bool IsRunning { get; }
    void Start(Func<FlowFile, bool> ingest, CancellationToken ct);
    void Stop();
}
```

The `ingest` callback is `Fabric.IngestAndExecute` — it fans out to all entry-point processors and runs the pipeline synchronously. Returns `false` on backpressure.

### PollingSource (abstract base class)

For sources that poll an external system on a schedule. Subclasses implement `Poll()`, the base class handles scheduling (`PeriodicTimer`), lifecycle, backpressure, and error isolation.

```csharp
public abstract class PollingSource : IConnectorSource
{
    protected abstract List<FlowFile> Poll(CancellationToken ct);
    protected virtual void OnIngested(FlowFile ff) { }        // post-ingest hook
    protected virtual void OnRejected(FlowFile ff) { ... }     // backpressure hook
    // Start/Stop/IsRunning managed by base class
}
```

- Uses `PeriodicTimer` (no drift, cancellation-aware)
- One bad poll doesn't kill the source (exception caught + logged)
- `OnIngested()` — GetFile uses this to move files to `.processed/`
- `OnRejected()` — default returns FlowFile to pool
- No concurrency within Poll — sequential by design. Concurrency from Fabric semaphore.

### ListenHTTP (event-driven)
- Implements `IConnectorSource` directly
- Embedded Kestrel server on dedicated port (default 9092)
- Accepts POST with raw body or NiFi V3 binary (`application/octet-stream`)
- Extracts `X-Flow-*` headers as FlowFile attributes
- ASP.NET handles request threading — each request = one pipeline execution
- Returns 503 on backpressure

### GetFile (polling)
- Extends `PollingSource`
- `Poll()` scans directory for files matching pattern, returns FlowFiles
- `OnIngested()` moves file to `.processed/` subdirectory
- Backpressure: rejected files stay in place, retry next poll

---

## Backpressure

Single `SemaphoreSlim` on the Fabric (`max_concurrent_executions`, default 100). When the semaphore is full, `Execute()` returns `false` immediately. Sources handle this:
- ListenHTTP → 503 response
- GetFile → skip file, retry next poll

No per-stage backpressure. The semaphore limits total in-flight graph traversals across all sources.

---

## Hot Reload

1. FileSystemWatcher detects config.yaml change (500ms debounce)
2. Parse new config, run ConfigValidator + DAG validator
3. Diff processor defs: detect added, removed, updated processors and changed connections
4. Build new `PipelineGraph` — reuse unchanged processor instances
5. Atomic swap: `_graph = newGraph` (volatile write)
6. In-flight executions complete on old graph; new executions use new graph

No queue draining, no CancellationToken management. The graph swap is a single volatile write.

---

## Failure Handling

When `processor.Process(ff)` returns `FailureResult`:
1. Check if the processor has a `failure` connection
2. If yes → route the FlowFile to failure targets (same as success routing)
3. If no → log the error, increment error counter, return FlowFile to pool

When `processor.Process(ff)` throws an exception:
1. Catch the exception, log it
2. Same routing logic as FailureResult — follow `failure` connection or drop

No retries, no DLQ. Failures are handled within the graph by wiring error-handling processors.

---

## Management API

ASP.NET Minimal API on port 9091 (configurable):

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check + source status |
| `GET /metrics` | Prometheus text exposition |
| `GET /api/stats` | Total processed, active executions |
| `GET /api/flow` | Full DAG: processors, connections, stats, sources, providers |
| `GET /api/processors` | Processor name list |
| `GET /api/processor-stats` | Per-processor processed/error counts |
| `GET /api/connections` | Connection map |
| `GET /api/sources` | Source connector status |
| `GET /api/providers` | Provider list + states |
| `GET /api/provenance` | Recent provenance events |
| `POST /api/reload` | Trigger hot reload |
| `POST /api/processors/add` | Add processor at runtime |
| `DELETE /api/processors/remove` | Remove processor |
| `POST /api/processors/enable` | Enable processor |
| `POST /api/processors/disable` | Disable processor |

---

## StdLib Processors (17 types)

| Category | Processors |
|----------|------------|
| **Core** | UpdateAttribute, LogAttribute, FilterAttribute |
| **Routing** | RouteOnAttribute (predicate-based, uses Router.cs engine) |
| **Records** | ExtractRecordField, QueryRecord, TransformRecord (7 operations) |
| **JSON** | ConvertJSONToRecord, ConvertRecordToJSON |
| **Avro** | ConvertAvroToRecord, ConvertRecordToAvro |
| **CSV** | ConvertCSVToRecord, ConvertRecordToCSV |
| **Text** | ReplaceText, ExtractText, SplitText |
| **Sinks** | PutHTTP, PutFile, PutStdout |
| **Expressions** | EvaluateExpression (12 functions) |

**Source connectors:** ListenHTTP, GetFile, GenerateFlowFile

---

## Object Pooling

Hot-path types are object-pooled to minimize GC pressure:
- `FlowFile` — `Pool<FlowFile>.Rent()` / `Return()`
- `SingleResult`, `MultipleResult`, `RoutedResult`, `FailureResult` — pooled
- `AttributeMap` — pooled overlay nodes
- `Content.Raw` uses `ArrayPool<byte>` for backing storage

Pool is thread-local (`[ThreadStatic]`), 256 items per thread, no CAS contention.

---

## File Layout (C# runtime)

```
caravan-flow-csharp/                           5,277 lines
├── CaravanFlow/
│   ├── Core/
│   │   ├── Types.cs          489  — FlowFile, Content, ProcessorResult, Pool<T>
│   │   ├── Providers.cs      305  — IProvider, ProcessorContext, ScopedContext, Logging, Provenance
│   │   ├── ContentStore.cs   204  — IContentStore, FileContentStore, MemoryContentStore, cleanup
│   │   ├── JsonRecord.cs     150  — JSON ↔ Record conversion
│   │   ├── Avro.cs           103  — Avro schema + GenericRecord
│   │   ├── Connectors.cs      33  — IConnectorSource interface
│   │   └── Binary.cs          35  — Binary encoding helpers
│   ├── Fabric/
│   │   ├── Fabric.cs         801  — Pipeline executor, Execute(), hot reload, lifecycle
│   │   ├── ApiHandler.cs     280  — REST management API
│   │   ├── Metrics.cs        264  — Prometheus metrics, ConfigValidator, DagValidator
│   │   ├── FlowFileV3.cs     180  — NiFi FlowFile V3 binary serde
│   │   ├── Router.cs         153  — Predicate rules engine (for future RouteOnAttribute)
│   │   ├── Processors.cs     138  — BuiltinProcessors.RegisterAll()
│   │   └── Registry.cs        49  — Processor type registry + factory
│   ├── StdLib/
│   │   ├── AvroBinary.cs     403  — Hand-rolled Avro binary codec (AOT-safe)
│   │   ├── Processors.cs     313  — UpdateAttribute, LogAttribute, PutHTTP, PutFile, PutStdout
│   │   ├── Sources.cs        240  — ListenHTTP, GetFile connector sources
│   │   ├── ExpressionProcessors.cs  239  — EvaluateExpression, TransformRecord
│   │   ├── RecordProcessors.cs      220  — Avro/CSV/JSON record conversion processors
│   │   ├── CsvRecord.cs     207  — CSV ↔ Record conversion
│   │   └── TextProcessors.cs 160  — ReplaceText, ExtractText, SplitText
│   └── Program.cs            311  — Entry point, config loading, ASP.NET setup, benchmarks
├── tests/Tests/
│   └── TestSuite.cs         3200+ — 438 tests (core types, processors, codecs, DAG, failure, e2e)
├── config.yaml                     — Flow definition
└── caravan.toml                       — Build config (caravan-csharp reads this)
```

---

## Deployment Modes

### Standalone (current)
Single binary, zero deps. Config from YAML. Best for dev, test, edge, single-machine workloads.

```
[ListenHTTP :9092] → [caravan-flow process] → [PutFile / PutHTTP]
```

### Multi-instance (Phase 3)
Multiple caravan-flow processes connected via NATS. Break flows into fast/slow segments. Each segment is a separate process (pod) with its own config.

```
[caravan-flow A]                    [caravan-flow B]
  ListenHTTP → parse → PutNats ──→ GetNats → transform → PutFile
                  (fast segment)       (slow segment)
```

### K8s-managed (Phase 3b)
Operator deploys ProcessorGroup CRDs as pods. Flow CRD defines global topology.
