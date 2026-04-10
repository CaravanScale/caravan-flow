# Zinc Flow

Lightweight, cloud-native data flow engine inspired by Apache NiFi and Apache Camel. Built in [Zinc](https://github.com/ZincScale/zinc) to dogfood the zinc-go transpiler.

## What is it?

NiFi's processor model with Camel's direct pipeline execution and Zinc's simplicity.

- **Processors** are Zinc classes implementing `ProcessorFn` (1-in, 1-out)
- **DAG connections** wire processors into a directed graph — FlowFiles flow synchronously through the pipeline
- **Failure routing** — processors can return `FailureResult`, routed via "failure" connections to error handlers
- **Providers** give processors scoped access to shared infrastructure (content store, config, logging)
- **Lifecycle management** — enable/disable processors and providers at runtime
- **Backpressure** — semaphore-gated concurrent executions, sources get 503 when full
- **Hot reload** — atomic pipeline graph swap on config change, zero downtime
- **Three runtimes:** Go (goroutines, 11MB), C# .NET 10 (AOT, 16MB, 2M+ ff/s), Python 3.14t (14MB)

## Quick Start

```bash
# Build and run
./zinc build
./zinc-out/zinc-flow

# Ingest a FlowFile
curl -X POST http://localhost:9092/ -d '{"hello":"world"}' -H 'X-Flow-type: order'

# Check stats
curl http://localhost:9091/api/stats
curl http://localhost:9091/api/flow
curl http://localhost:9091/api/providers
```

## Project Structure

```
zinc-flow/
├── src/
│   ├── core/
│   │   ├── flowfile.zn        — FlowFile data model
│   │   ├── result.zn          — ProcessorResult sealed type + ProcessorFn interface
│   │   ├── content.zn         — Content sealed type (Raw, Records, Claim)
│   │   ├── contentstore.zn    — ContentStore + FileContentStore + MemoryContentStore
│   │   ├── context.zn         — Provider interface, ComponentState, ProcessorContext
│   │   ├── providers.zn       — ConfigProvider, LoggingProvider, ContentProvider
│   │   ├── scoped_context.zn  — ScopedContext (per-processor provider isolation)
│   │   ├── avro.zn            — Avro schema, GenericRecord
│   │   ├── record.zn          — RecordReader/RecordWriter interfaces
│   │   ├── json_record.zn     — JSON record serde
│   │   └── binary.zn          — V3 binary encoding helpers
│   ├── fabric/
│   │   ├── runtime/runtime.zn — Fabric engine (pipeline graph, direct execution)
│   │   ├── api/handlers.zn    — REST API (processors, connections, providers)
│   │   ├── registry/registry.zn — Processor registry + factory pattern
│   │   ├── router/             — Predicate routing engine (for RouteOnAttribute)
│   │   ├── source/http.zn     — HTTP ingest source
│   │   ├── delivery/http.zn   — HTTP delivery adapter
│   │   └── model/             — V3 serde + result JSON
│   ├── processors/builtin.zn  — 5 built-in processors
│   └── main.zn                — Bootstrap + HTTP server
├── zinc-flow-csharp/          — C# .NET 10 runtime (17 processors, 395 tests)
├── zinc-flow-python/          — Python 3.14t runtime
├── test/
│   ├── test_main.zn           — 30 tests, 137+ assertions, 10 scenarios
│   └── test_helpers.zn        — Test utilities + assertions
├── config.yaml                — Flow definition (processors, connections)
├── zinc.toml                  — Project config
└── TODO.md                    — Roadmap
```

## Architecture

```
Source (ListenHTTP / GetFile)
    ↓
IngestAndExecute(FlowFile)
    ↓
Entry-point processors (fan-out if multiple)
    ↓
Execute loop (iterative work-stack, depth-first):
    Processor.Process(ff) → ProcessorResult
        SingleResult   → follow "success" connections
        MultipleResult → fan-out each output to "success" connections
        RoutedResult   → follow named relationship connections
        DroppedResult  → stop
        FailureResult  → follow "failure" connections (or log+drop)
    ↓
Sink processors (no outgoing connections) — terminal
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | (source port) | Ingest FlowFile via ListenHTTP (raw body or V3 binary) |
| GET | /health | Health check + source status |
| GET | /api/stats | Processing counters + active executions |
| GET | /api/processors | List active processors |
| GET | /api/processor-stats | Per-processor processed/error counts |
| GET | /api/flow | Full flow graph (processors + connections + stats) |
| GET | /api/connections | Processor connection map |
| GET | /api/providers | List providers and states |
| GET | /api/provenance | Recent provenance events |
| GET | /api/sources | List connector sources |
| GET | /metrics | Prometheus metrics |
| POST | /api/processors/enable | Enable a processor |
| POST | /api/processors/disable | Disable a processor |
| POST | /api/processors/add | Add processor at runtime |
| DELETE | /api/processors/remove | Remove a processor |
| POST | /api/reload | Hot reload config.yaml |
| POST | /api/providers/enable | Enable a provider |
| POST | /api/providers/disable | Disable with cascade to dependent processors |

## Runtimes

| Runtime | Binary | Throughput | Best for |
|---|---|---|---|
| **Go** (zinc-go transpiled) | 11MB static | 599K ff/s | Edge, embedded, performance-critical |
| **C# .NET 10** (zinc-csharp AOT) | 16MB stripped | 2M+ ff/s | Maximum throughput, .NET ecosystems |
| **Python 3.14t** (free-threaded) | 14MB native | 95K ff/s | ML/pandas integration, Python orgs |

## Status

**Phase 2 — Useful Standalone** — complete for C# runtime. Direct pipeline executor, 17 processors, hot reload, Prometheus metrics, 438 tests. See [TODO.md](TODO.md) for the roadmap.

## Design

- [docs/architecture.md](docs/architecture.md) — canonical architecture document (execution model, types, pipeline graph, providers, deployment)
- [docs/data-engine-comparison.md](docs/data-engine-comparison.md) — comparison with NiFi, DeltaFi, Flink, Spark, Beam
- [docs/nifi-component-analysis.md](docs/nifi-component-analysis.md) — NiFi processor catalog (reference for future StdLib expansion)

## Related

- [Zinc Language](https://github.com/ZincScale/zinc) — the language Zinc Flow is written in
- [zinc-csharp](https://github.com/ZincScale/zinc/tree/master/zinc-csharp) — C# build backend (installs .NET, reads zinc.toml, produces AOT binaries)
