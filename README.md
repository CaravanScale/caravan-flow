# Zinc Flow

Lightweight, cloud-native data flow engine inspired by Apache NiFi. Built in [Zinc](https://github.com/ZincScale/zinc) to dogfood the zinc-go transpiler.

## What is it?

NiFi's processor model — with transactional delivery guarantees, backpressure, and Zinc's simplicity.

- **Processors** are Zinc classes implementing `ProcessorFn` (1-in, 1-out)
- **Queues** between processors provide transactional claim/ack/nack with visibility timeout
- **IRS routing** evaluates predicate rules and fans out to multiple destinations (all-or-nothing)
- **Providers** give processors scoped access to shared infrastructure (content store, config, logging)
- **Lifecycle management** — enable/disable processors and providers at runtime with graceful drain
- **Backpressure** — bounded queues propagate pressure backwards through the flow graph
- **DLQ** — inspectable dead letter queue with replay to source queue
- Zinc transpiles to Go — goroutines, typed channels, native binaries (~2.5MB)

## Quick Start

```bash
# Build and run
./zinc build
./zinc-out/zinc-flow

# Ingest a FlowFile
curl -X POST http://localhost:9091/ingest -d '{"hello":"world"}' -H 'X-Flow-type: order'

# Check stats
curl http://localhost:9091/api/stats
curl http://localhost:9091/api/queues
curl http://localhost:9091/api/dlq
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
│   │   ├── queue.zn           — FlowQueue (transactional, bounded, visibility timeout)
│   │   ├── session.zn         — ProcessSession (claim → process → route → ack)
│   │   ├── dlq.zn             — Dead letter queue (inspect, replay)
│   │   ├── avro.zn            — Avro schema, GenericRecord
│   │   ├── record.zn          — RecordReader/RecordWriter interfaces
│   │   ├── json_record.zn     — JSON record serde
│   │   └── binary.zn          — V3 binary encoding helpers
│   ├── fabric/
│   │   ├── runtime/runtime.zn — Fabric engine (queues, sessions, lifecycle, async loops)
│   │   ├── api/handlers.zn    — REST API (processors, routes, queues, DLQ, providers)
│   │   ├── registry/registry.zn — Processor registry + factory pattern
│   │   ├── router/             — IRS predicate routing engine
│   │   ├── source/http.zn     — HTTP ingest source
│   │   ├── delivery/http.zn   — HTTP delivery adapter
│   │   └── model/             — V3 serde + result JSON
│   ├── processors/builtin.zn  — 5 built-in processors
│   └── main.zn                — Bootstrap + HTTP server
├── test/
│   ├── test_main.zn           — 29 tests, 128+ assertions, 9 scenarios
│   └── test_helpers.zn        — Test utilities + assertions
├── docs/
│   └── design-providers.md    — Flow engine architecture (canonical design doc)
├── config.yaml                — Flow definition (processors, routes, providers)
├── zinc.toml                  — Project config
└── TODO.md                    — Roadmap
```

## Architecture

```
HTTP POST /ingest
    ↓
Ingest Queue (bounded, backpressure → 503)
    ↓
IRS Routing (predicate rules, all-or-nothing fan-out)
    ↓
Processor Input Queue ──claim──→ [Processor] ──result──→ IRS ──offer──→ Next Queue(s)
    ↑                                                        │
    └──────────── nack (backpressure or failure) ────────────┘
                                                              │
                                                    retry exceeded → DLQ
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /ingest | Ingest FlowFile (raw body or V3 binary) |
| GET | /health | Health check + DLQ count |
| GET | /api/stats | Processing counters |
| GET | /api/processors | List active processors |
| GET | /api/flow | Full flow graph (processors + routes + stats) |
| GET | /api/queues | Queue depths per processor |
| GET | /api/dlq | DLQ entries with details |
| POST | /api/dlq/replay | Replay DLQ entry to source queue |
| POST | /api/dlq/replay-all | Replay all DLQ entries |
| GET | /api/providers | List providers and states |
| POST | /api/processors/enable | Enable a processor |
| POST | /api/processors/disable | Disable with drain |
| POST | /api/providers/enable | Enable a provider |
| POST | /api/providers/disable | Disable with cascade to dependent processors |
| POST | /api/processors/add | Add processor at runtime |
| POST | /api/routes/add | Add routing rule |

## Status

**Phase 1.5 — Flow Engine** — complete. See [TODO.md](TODO.md) for the full roadmap.

## Design

The canonical design document is [docs/design-providers.md](docs/design-providers.md) — covers transactional queues, provider lifecycle, IRS fan-out, backpressure, DLQ, and bootstrap sequence.

## Related

- [Zinc Language](https://github.com/ZincScale/zinc) — the language Zinc Flow is written in
