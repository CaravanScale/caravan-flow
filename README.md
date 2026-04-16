# Zinc Flow

Lightweight, cloud-native data flow engine inspired by Apache NiFi and Apache Camel. Built in [Zinc](https://github.com/ZincScale/zinc) and compiled via [zinc-csharp](https://github.com/ZincScale/zinc/tree/master/zinc-csharp) to a .NET 10 AOT binary.

## What is it?

NiFi's processor model with Camel's direct pipeline execution and Zinc's simplicity.

- **Processors** are Zinc classes implementing `ProcessorFn` (1-in, 1-out)
- **DAG connections** wire processors into a directed graph — FlowFiles flow synchronously through the pipeline
- **Failure routing** — processors can return `FailureResult`, routed via "failure" connections to error handlers
- **Providers** give processors scoped access to shared infrastructure (content store, config, logging)
- **Lifecycle management** — enable/disable processors and providers at runtime
- **Backpressure** — semaphore-gated concurrent executions, sources get 503 when full
- **Hot reload** — atomic pipeline graph swap on config change, zero downtime
- **C# .NET 10 AOT runtime** — 16MB stripped binary, 2M+ ff/s throughput

## Quick Start

```bash
cd zinc-flow-csharp
zinc build .
./zinc-out/zinc-flow

# Run the test suite
zinc test .

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
├── zinc-flow-csharp/          — C# .NET 10 runtime (17 processors, 395 tests)
├── processors/                — additional processor packages
├── config.yaml                — Flow definition (processors, connections)
├── zinc.toml                  — Project config
├── docs/                      — Design and reference docs
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

## Status

**Phase 2 — Useful Standalone** — direct pipeline executor, 17 processors, hot reload, Prometheus metrics, 438 tests. See [TODO.md](TODO.md) for the roadmap.

## Design

- [docs/architecture.md](docs/architecture.md) — canonical architecture document (execution model, types, pipeline graph, providers, deployment)
- [docs/csharpism-audit.md](docs/csharpism-audit.md) — zinc/C# idiom alignment audit
- [docs/data-engine-comparison.md](docs/data-engine-comparison.md) — comparison with NiFi, DeltaFi, Flink, Spark, Beam
- [docs/nifi-component-analysis.md](docs/nifi-component-analysis.md) — NiFi processor catalog (reference for future StdLib expansion)
- [docs/product-research-2026-04.md](docs/product-research-2026-04.md) — product research notes

## Related

- [Zinc Language](https://github.com/ZincScale/zinc) — the language Zinc Flow is written in
- [zinc-csharp](https://github.com/ZincScale/zinc/tree/master/zinc-csharp) — C# build backend (installs .NET, reads zinc.toml, produces AOT binaries)
