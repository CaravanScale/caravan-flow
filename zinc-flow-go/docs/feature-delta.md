# zinc-flow-go ↔ zinc-flow-csharp feature delta

Both repos are implementations of the same conceptual flow-processing
engine (NiFi-like). Different languages, different maturity levels on
different subsystems. This doc is feature-oriented, not code-oriented.

## Subsystem coverage matrix

| Subsystem | zinc-flow-go | zinc-flow-csharp | Notes |
|---|---|---|---|
| **FlowFile model** | ✅ (core/flowfile, fabric/model/flowfile) | ✅ (Fabric/FlowFileV3.cs) | C# has v3 explicitly versioned |
| **Content + ContentStore** | ✅ | ✅ | Parity |
| **Record abstraction** | ✅ (core/record) | ✅ (Core/Record.cs) | Parity |
| **JsonRecord** | ✅ | ✅ | Parity |
| **Binary encoding** | ✅ (core/binary) | ✅ (Core/Binary.cs) | Parity |
| **Result type** | ✅ (core/result, fabric/model/result) | Implicit in C# | C# uses exceptions + nullable |
| **Connectors** | ✅ (core/connector) | ✅ (Core/Connectors.cs) | Parity |
| **Providers** | ✅ (core/providers, context, scoped_context) | ✅ (Core/Providers.cs) | zinc has explicit ScopedContext / Context split |
| **Router** | ✅ (fabric/router/engine + rules) | ✅ (Fabric/Router.cs) | zinc splits engine/rules into two files |
| **Registry** | ✅ (fabric/registry) | ✅ (Fabric/Registry.cs) | Parity |
| **Runtime / Fabric** | ✅ (fabric/runtime) | ✅ (Fabric/Fabric.cs) | Parity |
| **API handlers** | ✅ (fabric/api/handlers) | ✅ (Fabric/ApiHandler.cs) | Parity |
| **HTTP source** | ✅ (fabric/source/http) | ✅ (StdLib/ListenHTTP.cs) | Parity |
| **HTTP delivery** | ✅ (fabric/delivery/http) | ❌ | C# missing outbound HTTP delivery |
| **Generate source (synth test input)** | ✅ (fabric/source/generate) | ❌ | zinc-only |
| **Source registry** | Implicit via registry | ✅ (Fabric/SourceRegistry.cs) | C# has explicit registry |
| **Pipeline graph** | ✅ (core/pipeline_graph) | ❌ | zinc-only — graph/topology model |
| **Queue** | ✅ (core/queue) | ❌ | zinc-only — explicit queue abstraction |
| **DLQ (dead-letter queue)** | ✅ (core/dlq) | ❌ | zinc-only |
| **Session** | ✅ (core/session) | ❌ | zinc-only |
| **Metrics** | ❌ | ✅ (Fabric/Metrics.cs) | csharp-only |
| **Flow validator** | ❌ | ✅ (Fabric/FlowValidator.cs) | csharp-only |
| **YAML parser/emitter** | ❌ | ✅ (Core/YamlParser.cs + YamlEmitter.cs) | csharp-only — config format |
| **Config overlay** | ❌ | ✅ (Core/Overlay.cs) | csharp-only |
| **Config helpers** | ❌ | ✅ (Core/ConfigHelpers.cs) | csharp-only |
| **JSON AOT context** | ❌ | ✅ (Core/JsonContext.cs) | .NET AOT JSON source generator |

## Standard library (processors / sources / codecs)

Feature-level: what processor types and codecs each side ships.

### Processors

| Processor | zinc-flow-go | zinc-flow-csharp |
|---|---|---|
| PutStdout | ✅ | ✅ (in Processors.cs) |
| PutFile | ✅ | ✅ (in Processors.cs) |
| LogAttribute | ✅ | ✅ |
| UpdateAttribute | ✅ | ✅ |
| FilterAttribute | ✅ | ✅ |
| RouteOnAttribute | ✅ | ✅ |
| ExtractRecordField | ✅ | ✅ (in RecordProcessors) |
| TransformRecord | ✅ | ✅ (in RecordProcessors) |
| TextProcessors (split/trim/replace etc.) | ✅ (processors/text_processors) | ✅ (StdLib/TextProcessors.cs) |
| **Expression-engine processor** (EL evaluation) | ❌ | ✅ (StdLib/ExpressionProcessors.cs) |
| **Compression processors** (gzip, snappy, etc.) | ❌ | ✅ (StdLib/CompressionProcessors.cs) |

### Sources

| Source | zinc-flow-go | zinc-flow-csharp |
|---|---|---|
| GenerateFlowFile (test) | ✅ | ❌ |
| ListenHTTP | ✅ | ✅ |
| HTTP delivery (outbound) | ✅ | ❌ |

### Schema / codec

| Feature | zinc-flow-go | zinc-flow-csharp |
|---|---|---|
| Avro binary encoding | ✅ (core/avro) | ✅ (StdLib/AvroBinary.cs) |
| **Avro OCF (container files)** | ❌ | ✅ (StdLib/AvroOCF.cs) |
| **Avro full schema parser** | ❌ | ✅ (StdLib/AvroSchema.cs) |
| **Avro logical type helpers** (date/timestamp/decimal) | ❌ | ✅ (Core/LogicalTypeHelpers.cs) |
| **Embedded schema registry** | ❌ | ✅ (StdLib/EmbeddedSchemaRegistry.cs) |
| **Schema registry handler** (wire protocol) | ❌ | ✅ (Fabric/SchemaRegistryHandler.cs) |
| **Schema resolver** (version reconciliation) | ❌ | ✅ (StdLib/SchemaResolver.cs) |
| **Expression engine** (NiFi EL) | ❌ | ✅ (StdLib/ExpressionEngine.cs) |
| **CSV record codec** | ❌ | ✅ (StdLib/CsvRecord.cs) |

## Summary of the delta

### What zinc-flow-go has that C# doesn't

Runtime/topology primitives — the "inside" of the engine:
- **DLQ** — dead-letter queue for failed flowfiles
- **Session** — transactional unit-of-work abstraction
- **Pipeline graph** — topology model, flow-as-graph
- **Queue** — explicit queue between processors
- **Scoped context / Context split** — process-level vs flow-level provider scoping
- **HTTP outbound delivery** — sink that POSTs flowfiles
- **Generate source** — synthetic flowfile generator for testing

### What zinc-flow-csharp has that zinc-flow-go doesn't

Data-plane depth — the "content" side:
- **Full Avro stack** — OCF, full schema parser, logical types, schema registry (embedded + wire), resolver
- **Expression engine + processors** — NiFi EL evaluation on attributes
- **CSV codec**
- **Compression** — gzip/snappy/etc. as processors
- **YAML parser + emitter** — config format
- **Config overlay** — layered config
- **Metrics**
- **Flow validator** — static check of flow topology

### What both have — core runtime

FlowFile, ContentStore, Record + JsonRecord, Binary codec, Router,
Registry, Providers, Connectors, runtime/fabric glue, API handlers,
ListenHTTP source, attribute processors (filter/log/update/route),
record processors (extract/transform), text processors.

## Observations for tomorrow

1. **The two codebases aren't behind/ahead — they're differently deep.**
   zinc-flow-go is more thorough on runtime topology (queues, sessions,
   DLQ, scoping); zinc-flow-csharp is more thorough on data-plane
   codecs (Avro stack, expression engine, compression).

2. **Likely convergence path:** pick which side's features should be
   canonical, port the missing pieces across both. Natural split —
   runtime primitives (DLQ, session, queue, pipeline graph) come from
   zinc-flow-go; data-plane features (Avro, EL, CSV, compression,
   schema) come from zinc-flow-csharp.

3. **Error/config models differ.** C# uses exceptions (`ConfigException`);
   zinc uses error values (`errors.ConfigError`). If these converge, the
   C# side is the one to change — Zinc's error model is now the spec
   (see `zinc-go/docs/error-handling.md` + recent compiler widening
   work).

4. **YAML/config handling is csharp-only today.** If we expect flows to
   be configured in YAML (NiFi-style), zinc-flow-go needs a YAML parser
   — today the zinc stdlib only has `config` (probably TOML-ish via
   viper in its current form; worth checking).

5. **FlowFile versioning.** C# explicitly has `FlowFileV3`; zinc has
   `fabric/model/flowfile`. Worth checking if zinc's flowfile is v3
   conformant or an earlier model.
