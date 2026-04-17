# caravan-flow-csharp

C# / .NET 10 port of the caravan-flow engine. Uses ThreadStatic object pools, ArrayPool byte buffers, ref-counted Content, and AttributeMap overlay chains to achieve 2M+ ff/s with zero GC during execution.

**Start here:** [`docs/getting-started.md`](docs/getting-started.md) — install, CLI, config structure, processor catalog, expression language, management API.

**Examples:** [`examples/`](examples/) — five validated configs covering hello-world, JSON pipelines, Avro OCF, schema evolution, and registry-backed schemas.

## Quick Start

```bash
# Install caravan-csharp (one-stop shop — installs .NET 10 + build tool)
curl -LsSf https://raw.githubusercontent.com/CaravanScale/caravan/master/caravan-csharp/install.sh | bash

# Build Native AOT binary
caravan-csharp build

# Run
caravan-csharp run

# Run benchmarks
./build/CaravanFlow --bench

# JIT mode (fast iteration)
caravan-csharp build --jit
caravan-csharp run --jit

# Run tests (separate project, never in production binary)
caravan-csharp test

# Diagnostics
caravan-csharp doctor

# Clean
caravan-csharp clean
```

All build configuration lives in `caravan.toml` — the `.csproj` is generated and gitignored. You never touch XML.

## Project Structure

```
caravan-flow-csharp/
├── caravan.toml              — Project config (framework, AOT, NuGet deps)
├── config.yaml            — Runtime flow config (processors, routes, providers)
├── CaravanFlow/              — C# source
│   ├── Core/
│   │   ├── Types.cs       — Pool<T>, AttributeMap, FlowFile, Content (ref-counted), ProcessorResults
│   │   ├── FlowQueue.cs   — Transactional queue: ArrayPool backing, pooled entries
│   │   ├── ProcessSession.cs — Claim→process→route→ack with object return-to-pool
│   │   ├── DLQ.cs         — Dead letter queue
│   │   ├── Providers.cs   — Provider interface, ProcessorContext, ScopedContext
│   │   ├── ContentStore.cs — FileContentStore, MemoryContentStore, offload to claim
│   │   ├── Avro.cs        — Schema, GenericRecord
│   │   ├── JsonRecord.cs  — JSON record serde
│   │   └── Binary.cs      — Big-endian encoding helpers
│   ├── Fabric/
│   │   ├── Fabric.cs      — Runtime engine (queues, sessions, lifecycle, async loops)
│   │   ├── Router.cs      — RulesEngine with BaseRule + CompositeRule (AND/OR)
│   │   ├── Processors.cs  — 5 built-in processors
│   │   ├── Registry.cs    — Processor registry + factory pattern
│   │   ├── FlowFileV3.cs  — NiFi FlowFile V3 binary pack/unpack
│   │   ├── HttpSource.cs  — HTTP ingest (raw + V3 binary)
│   │   └── ApiHandler.cs  — 21 management API endpoints
│   └── Program.cs         — Server mode + benchmarks (--bench)
├── tests/
│   ├── caravan.toml          — Test project config (separate from production)
│   └── Tests/
│       ├── Program.cs     — Test entry point
│       └── TestSuite.cs   — 149 assertions across all modules
├── build/                 — AOT binary output (gitignored)
└── docs/
    └── design-pooling.md  — Pool types, ref counting, GC analysis
```

## caravan.toml → .csproj

The `caravan-csharp` tool reads `caravan.toml` and generates `.csproj` on every build:

```toml
[project]
name = "CaravanFlow"
version = "0.1.0"
source_dir = "CaravanFlow"
sdk = "Microsoft.NET.Sdk.Web"

[csharp]
framework = "net10.0"
lang_version = "latest"
nullable = true
implicit_usings = true
unsafe = true

[csharp.aot]
enabled = true
strip_symbols = true
invariant_globalization = true
optimization = "Size"
stack_traces = false
trim_metadata = true

[csharp.gc]
server = true

[csharp.nuget]
packages = ["YamlDotNet:16.3.0"]
```

Tests are a separate project (`tests/caravan.toml`) with a `ProjectReference` to the main project. Production binary has zero test code.

## Memory Borrowing / Pooling Strategy

The engine processes FlowFiles through a claim→process→route→ack pipeline. Every step reuses pooled objects — zero allocations in steady state.

### 1. ThreadStatic Object Pool (`Pool<T>`)

Zero-contention, zero-CAS object pool using `[ThreadStatic]` arrays. Each thread maintains its own pool of up to 256 objects per type.

Pooled types: `FlowFile`, `QueueEntry`, `SingleResult`, `RoutedResult`, `MultipleResult`, `FailureResult`, `AttributeMap`, `Raw`

```csharp
// ~2-3ns rent/return (array index + decrement)
var ff = Pool<FlowFile>.Rent();
Pool<FlowFile>.Return(ff);
```

### 2. Ref-Counted Content

Content (Raw byte buffers) is shared across FlowFiles via `WithAttribute`. A non-atomic ref count tracks ownership:

- `WithAttribute` → increments ref count (shared Content, new FlowFile shell)
- `FlowFile.Return` → decrements ref count; when zero, returns ArrayPool bytes and pools Raw shell
- Non-atomic: safe because FlowFiles move through pipeline stages sequentially

```
FF1.Create(data) → Raw refCount=1
FF2 = WithAttribute(FF1) → Raw refCount=2
Return(FF1) → refCount=1
Return(FF2) → refCount=0 → ArrayPool.Return(bytes) + Pool<Raw>.Return(shell)
```

### 3. AttributeMap Overlay Chain

`WithAttribute` creates an immutable linked overlay instead of cloning the Dictionary:

```
Before: new Dictionary(parent) + copy  → ~200 bytes/call
After:  overlay node (parent, key, value) → ~40 bytes/call, pooled
```

### 4. ArrayPool for Content Bytes and Queue Backing

- `Raw` content rents from `ArrayPool<byte>.Shared` (avoids LOH for >85KB payloads)
- `FlowQueue._items` rented from `ArrayPool<QueueEntry?>.Shared`
- Ref counting ensures bytes are returned when last FlowFile reference is released

### 5. Zero-Alloc Routing

`ProcessSession._destBuffer` is a pre-allocated `List<string>` reused across all `Execute()` calls. `RulesEngine.GetDestinations()` writes into caller's buffer.

## Hot Path Allocation Summary

| Operation | Before | After (pooled) |
|---|---|---|
| `WithAttribute` per hop | new Dict + copy | 1 pooled AttributeMap overlay |
| `FlowFile` per process | `new FlowFile()` | `Pool<FlowFile>.Rent()` |
| `QueueEntry` per claim | `new QueueEntry()` | mutate in-place (zero alloc) |
| `QueueEntry` per offer | `new QueueEntry()` | `Pool<QueueEntry>.Rent()` |
| `SingleResult` per process | `new SingleResult()` | `Pool<SingleResult>.Rent()` |
| `RoutedResult` per process | `new RoutedResult()` | `Pool<RoutedResult>.Rent()` |
| `MultipleResult` per process | `new MultipleResult()` | `Pool<MultipleResult>.Rent()` |
| `FailureResult` per process | `new FailureResult()` | `Pool<FailureResult>.Rent()` |
| Raw content bytes | `new byte[]` | `ArrayPool<byte>.Rent()` + ref-counted return |
| Raw object shell | `new Raw()` | `Pool<Raw>.Rent()` + ref-counted return |
| Queue backing array | `new QueueEntry[]` | `ArrayPool.Rent()` |
| Destination list | `new List<string>()` | reuse `_destBuffer` |

## Benchmark Results

2-hop pipeline (AddAttribute→route→AddAttribute), single-threaded, Native AOT, .NET 10, Server GC.

### Session Throughput

| Size | Time | Rate | GC during execution |
|---|---|---|---|
| 10K ff | 4-6ms | 2.0-2.5M ff/s | gc0: 0 |
| 50K ff | 25ms | 2.0M ff/s | gc0: 0 |
| 100K ff | 46-68ms | 1.5-2.2M ff/s | gc0: 0 |
| 500K ff | 256ms | 1.95M ff/s | gc0: 0 |

### Queue Operations (100K)

| Operation | Rate |
|---|---|
| Offer | 2.0-2.8M ops/s |
| Claim+Ack | 16-25M ops/s |

### Binary Size

```
16 MB — stripped ELF x86-64, .NET 10 AOT, Web SDK
        IlcOptimizationPreference=Size, InvariantGlobalization,
        no stack traces, trimmed metadata
```

## Key Design Decisions

1. **ThreadStatic over ConcurrentBag** — ConcurrentBag has O(threadCount) scan on cold pool. ThreadStatic array is 10-50x faster for single-thread-per-processor model.

2. **Non-atomic ref counting** — FlowFiles move through pipeline stages sequentially. No concurrent access to the same Content, so `_refCount++`/`_refCount--` is safe without Interlocked.

3. **Overlay chain over FrozenDictionary** — `With()` is O(1), lookup is O(depth). For 2-4 hop pipelines, chain depth is tiny and cache-friendly.

4. **In-place mutation for Claim** — QueueEntry moves from visible to invisible. Since we're inside the lock, mutating `ClaimedAt` in-place eliminates one allocation per dequeue.

5. **Separate test project** — Tests have their own `caravan.toml` and `ProjectReference` to the main project. Production AOT binary has zero test code.

6. **Server GC** — Dedicated GC threads, larger generation sizes, better for throughput workloads.

## Related

- [caravan-csharp build tool](https://github.com/CaravanScale/caravan/tree/master/caravan-csharp) — the build tool used by this project
- [design-pooling.md](docs/design-pooling.md) — deep dive on .NET pool types and choices
- [caravan-flow](https://github.com/CaravanScale/caravan-flow) — the Caravan/Go reference implementation
