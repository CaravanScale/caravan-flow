# Data Flow Engine Comparison

Comparison of data flow/processing engines and how they relate to caravan-flow's design.

## Engine Overview

| Engine | Model | Unit of Data | Processing | State | Primary Use |
|--------|-------|-------------|------------|-------|-------------|
| **NiFi** | Flow-based | FlowFile (content + attributes) | Event-at-a-time | Per-processor queues | Data routing, ETL, integration |
| **DeltaFi** | Flow-based | DeltaFile (content + metadata) | Event-at-a-time | Full provenance | Data normalization, transformation |
| **Flink** | Stream processing | Event/Record | Event-at-a-time | Distributed keyed state | Real-time analytics, CEP |
| **Spark** | Micro-batch | Row/DataFrame | Micro-batch (or continuous) | Checkpoint-based | Batch + streaming analytics |
| **Beam** | Unified SDK | PCollection element | Runner-dependent | Per-key state + timers | Portable batch + stream pipelines |
| **caravan-flow** | Flow-based | FlowFile (content + attributes) | Event-at-a-time | Stateless (direct pipeline) | Lightweight data routing, transform |

---

## Core Abstractions Compared

### Data Unit

| Engine | Name | Structure | Content Model |
|--------|------|-----------|---------------|
| **NiFi** | FlowFile | Attributes (Map<String,String>) + content (byte stream) + provenance | Content stored on disk, reference passed between processors. Large content never in memory. |
| **DeltaFi** | DeltaFile | Metadata + content segments + domains + provenance chain | Content stored in MinIO/S3, actions receive references. Segments allow multi-part content. |
| **Flink** | Event/Record | Typed POJO, Tuple, or Row | In-memory, serialized between operators via network buffers. Schema defined by TypeInformation. |
| **Spark** | Row | Typed columns in a DataFrame/Dataset schema | In-memory columnar format. Schema defined by StructType. |
| **Beam** | PCollection element | Any serializable type (Coder handles serialization) | In-memory, runner manages distribution. Schema support via Beam Schemas. |
| **caravan-flow** | FlowFile | Attributes (immutable overlay chain) + content (Raw/Record/Claim) + timestamp | Small content inline (ArrayPool), large content off-heap via content store claims. Object-pooled. |

**Key insight**: NiFi and DeltaFi store content externally (disk/object store) and pass references. This allows processing multi-GB files without OOM. Flink/Spark/Beam keep data in memory — optimized for high-throughput analytics, not large individual files.

### Processing Unit

| Engine | Name | Contract | Configuration |
|--------|------|----------|---------------|
| **NiFi** | Processor | `onTrigger(ProcessContext, ProcessSession)` — pull from input queue, push to relationships | Properties configured in UI, relationships define routing |
| **DeltaFi** | Action | Transform/Validate/Enrich/Egress — receives DeltaFile, returns result + new content | Actions are Java/Python classes, flows are YAML config |
| **Flink** | Operator | `ProcessFunction`, `MapFunction`, `FlatMapFunction`, etc. | Programmatic (Java/Scala/Python API) |
| **Spark** | Transformation | DataFrame operations: `select`, `filter`, `groupBy`, `map`, etc. | Programmatic (Scala/Python/SQL) |
| **Beam** | PTransform/DoFn | `DoFn.processElement()` — receives element, outputs to collectors | Programmatic (Java/Python/Go SDK) |
| **caravan-flow** | IProcessor | `Process(FlowFile): ProcessorResult` — returns Single/Multiple/Routed/Dropped/Failure | Classes registered at startup, flow graph is YAML config |

**Key insight**: NiFi/DeltaFi/caravan-flow are **configuration-driven** (processors are pre-built, graph is config). Flink/Spark/Beam are **code-driven** (you write the pipeline programmatically). This is a fundamental design split.

### Flow / Pipeline

| Engine | How flows are defined | Live modification |
|--------|----------------------|-------------------|
| **NiFi** | Visual drag-and-drop UI, stored as XML/JSON flow definition | Yes — full live editing, start/stop individual processors |
| **DeltaFi** | YAML flow definitions (ingress → transform → egress chains) | Yes — flows can be modified, actions toggled |
| **Flink** | Programmatic DAG (Java/Scala/Python), compiled and submitted as a job | No — requires job restart for topology changes |
| **Spark** | Programmatic DAG, submitted as a job | No — requires job restart |
| **Beam** | Programmatic DAG, submitted to a runner | No — requires pipeline restart |
| **caravan-flow** | YAML config (processors + connections) | Yes — hot reload with atomic pipeline graph swap, add/remove processors at runtime |

### State Management

| Engine | Approach | Fault Tolerance |
|--------|----------|-----------------|
| **NiFi** | FlowFile queues between processors, content repo on disk, provenance repo | Content repo survives restart, queues are durable |
| **DeltaFi** | Full DeltaFile provenance chain, content in object store, MongoDB for metadata | Resume from any failure point, replay capability |
| **Flink** | Distributed keyed state (RocksDB or heap), async incremental checkpoints | Exactly-once via checkpointing, barrier alignment |
| **Spark** | Checkpoint to HDFS/S3, WAL for sources | Exactly-once via checkpoint + WAL |
| **Beam** | Per-key state + timers, runner handles persistence | Runner-dependent (Flink runner = Flink guarantees) |
| **caravan-flow** | Stateless direct execution, content store for large data | Failure routing in-graph, replay from source. No inter-stage state. |

---

## Architecture Patterns

### NiFi — Flow-Based Programming
```
[Source] → Queue → [Processor A] → Queue → [Processor B] → Queue → [Sink]
                         ↓ (failure)
                    [Failure Queue]
```
- Processors pull from input queues, push to output relationships
- Backpressure via queue depth limits
- Content stored on disk, only references flow through queues
- Every FlowFile operation recorded in provenance

### DeltaFi — Action Pipeline
```
[Ingress Flow] → [Transform Action] → [Enrich Action] → [Egress Flow]
                                              ↓
                                    [Error / DLQ with resume]
```
- Three flow types: Ingress (data in), Transform (process), Egress (data out)
- Actions are typed: Transform, Validate, Enrich, Egress, TimedIngress
- Full provenance chain — every action's input/output stored
- Built on Kubernetes, Redis (queues), MongoDB (metadata), MinIO (content)

### Flink — Distributed Stream Processing
```
[Source] → [Map] → [KeyBy] → [Window] → [Aggregate] → [Sink]
                       ↓
              [State Backend (RocksDB)]
              [Checkpoint (HDFS/S3)]
```
- Operators process events one-at-a-time with access to keyed state
- Windows group events by time (tumbling, sliding, session)
- Watermarks track event-time progress for late data handling
- Backpressure propagates upstream via network buffer credit

### Spark Structured Streaming — Micro-Batch
```
[Source] → [DataFrame Transform] → [Aggregation] → [Sink]
                                        ↓
                               [State Store (HDFS)]
                               [Checkpoint (HDFS)]
```
- Treats stream as an unbounded table growing with each micro-batch
- Standard DataFrame/SQL operations
- Micro-batch (100ms+ latency) or continuous (1ms latency, limited ops)
- Exactly-once via checkpoint + WAL

### Beam — Portable Pipeline SDK
```
[PCollection] → [PTransform] → [PCollection] → [PTransform] → [PCollection]
                                                        ↓
                                               [Runner: Flink/Spark/Dataflow]
```
- Write once, run on any runner
- Unifies batch (bounded) and stream (unbounded) in one API
- Windowing + triggers + watermarks for stream semantics
- DoFn is the core per-element processing function

---

## NiFi's "Record" Concept Explained

NiFi's "record" is NOT a general data abstraction — it's a specific optimization for structured data:

**Without records**: A FlowFile contains raw bytes (CSV file, JSON array, etc.). Each processor must parse the entire content, transform it, and re-serialize. Format knowledge is baked into every processor.

**With records**: A RecordReader (controller service) parses bytes into structured records (rows with typed fields). Processors work on abstract Record objects. A RecordWriter serializes back to the desired format.

```
FlowFile (raw CSV bytes)
    → RecordReader (CSVReader) → Record[]
        → Processor works on Record objects (format-agnostic)
    → RecordWriter (JsonRecordSetWriter) → FlowFile (JSON bytes)
```

This is analogous to:
- Spark's DataFrame schema
- Flink's TypeInformation / RowType
- Beam's Schema

**For caravan-flow**: This is implemented — `RecordContent` holds `List<Dictionary<string, object?>>`, with ConvertJSONToRecord, ConvertAvroToRecord, ConvertCSVToRecord as readers and their reverse as writers. Processors like TransformRecord and EvaluateExpression work on abstract records, format-agnostic.

---

## Where caravan-flow Sits

caravan-flow combines ideas from **NiFi** (processor model, FlowFile abstraction), **Apache Camel** (direct pipeline execution, routes as config), and **NiFi Stateless** (ephemeral, no inter-stage queues):

- Configuration-driven flow graphs (not programmatic pipelines)
- Processors as pre-built components, graph as YAML config
- Event-at-a-time processing (not micro-batch)
- Direct synchronous execution (like Camel routes), no inter-stage queues
- Hot reload with atomic graph swap
- Failure routing in-graph via connections (no external DLQ)

Lighter weight than NiFi:
- No Java, no JVM — compiles to native binary (Go, C# AOT, Python)
- No distributed cluster — single process, scale by decomposition + NATS
- No visual UI (yet) — YAML + management API + Prometheus metrics
- No content repository — inline for small data, content store claims for large data

Key differences from Flink/Spark/Beam:
- Not a distributed analytics engine
- Not optimized for windowed aggregations or keyed state
- Not designed for terabyte-scale stateful computation
- Designed for data routing, transformation, and integration (the NiFi/Camel use case)
