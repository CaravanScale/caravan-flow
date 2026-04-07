# zinc-flow-csharp

C# / .NET 10 port of the zinc-flow engine, optimized with ArrayPool, MemoryPool, and ThreadStatic object pooling to minimize GC pressure on hot code paths.

## Quick Start

```bash
# JIT (development)
dotnet run -c Release

# Native AOT (1.4 MB static binary)
dotnet publish -c Release -r linux-x64
./bin/Release/net10.0/linux-x64/publish/ZincFlow
```

## Architecture

```
ZincFlow/
├── Core/
│   ├── Types.cs           — FlowFile, Content, AttributeMap, ProcessorResult, Pool<T>
│   ├── FlowQueue.cs       — Transactional queue: ArrayPool backing, pooled entries
│   ├── ProcessSession.cs  — Claim→process→route→ack with object return-to-pool
│   ├── DLQ.cs             — Dead letter queue
│   └── Providers.cs       — Provider interface, ProcessorContext, ScopedContext
├── Fabric/
│   ├── Router.cs          — RulesEngine predicate evaluation over AttributeMap
│   └── Processors.cs      — AddAttribute, LogProcessor
├── Program.cs             — Benchmarks with JIT warmup + GC stats
└── ZincFlow.csproj        — .NET 10, ServerGC, Native AOT, size-optimized
```

## Memory Borrowing / Pooling Strategy

The engine processes FlowFiles through a claim→process→route→ack pipeline. Each step
previously allocated new objects that became immediate garbage. The pooling architecture
eliminates nearly all hot-path allocations:

### 1. ThreadStatic Object Pool (`Pool<T>`)

Zero-contention, zero-CAS object pool using `[ThreadStatic]` arrays. Each thread maintains
its own pool of up to 256 objects per type. Rent is an array pop, return is an array push.

Pooled types: `FlowFile`, `QueueEntry`, `SingleResult`

```csharp
// Hot path: ~2ns rent/return (array index + decrement)
var ff = Pool<FlowFile>.Rent();
// ... use ff ...
Pool<FlowFile>.Return(ff);
```

### 2. AttributeMap Overlay Chain

`WithAttribute()` was the #1 allocation hotspot — it copied the entire `Dictionary<string,string>`
per processor hop. Replaced with an immutable linked overlay:

```
Before: new Dictionary(parent) + bucket array + element copy  → ~200 bytes/call
After:  new AttributeMap(parent, key, value)                  → ~40 bytes/call (5 fields)
```

The overlay chain walks parent links on `ContainsKey`/`TryGetValue`. For typical 2-4 attribute
depths, this is faster than dictionary hashing.

### 3. ArrayPool for Content Bytes

`Raw` content rents byte arrays from `ArrayPool<byte>.Shared` instead of allocating.
This keeps payloads off the LOH (Large Object Heap) for buffers >85KB and avoids
Gen2 collection triggers.

```csharp
// Rent from pool (may be slightly oversized — that's fine)
_rented = ArrayPool<byte>.Shared.Rent(data.Length);
data.CopyTo(_rented);

// Return when FlowFile is consumed
ArrayPool<byte>.Shared.Return(_rented);
```

### 4. ArrayPool for Queue Backing Array

`FlowQueue._items` is rented from `ArrayPool<QueueEntry?>`. Growth events return the
old array to the pool and rent a larger one, avoiding GC pressure from array resizing.

### 5. In-Place Mutation on Claim

Queue `Claim()` used to create a new `QueueEntry` copy to move from visible → invisible.
Now it mutates the existing entry in-place (just sets `ClaimedAt` timestamp), eliminating
one allocation per dequeue.

### 6. Reusable Destination Buffer

`ProcessSession._destBuffer` is a pre-allocated `List<string>` reused across all
`Execute()` calls. `RulesEngine.GetDestinations()` writes into this buffer via a
`Clear()` + `Add()` pattern instead of returning a new list.

## Hot Path Allocation Summary

| Operation | Before (v1) | After (pooled) |
|---|---|---|
| `WithAttribute` per hop | new Dict + buckets + copy | 1 AttributeMap node (~40B) |
| `FlowFile` per process | `new FlowFile()` | `Pool<FlowFile>.Rent()` |
| `QueueEntry` per claim | `new QueueEntry()` | mutate in-place (zero alloc) |
| `QueueEntry` per offer | `new QueueEntry()` | `Pool<QueueEntry>.Rent()` |
| `SingleResult` per process | `new SingleResult()` | `Pool<SingleResult>.Rent()` |
| Queue backing array | `new QueueEntry[]` | `ArrayPool.Rent()` |
| Content bytes | `new byte[]` | `ArrayPool<byte>.Rent()` |
| Destination list | `new List<string>()` | reuse `_destBuffer` |

## Benchmark Results

All benchmarks: 2-hop pipeline (AddAttribute "env"→"prod" → route → AddAttribute "done"→"true"),
single-threaded, measured after JIT warmup + forced Gen2 collection.

### Session Throughput (100K FlowFiles, 2 hops)

| Runtime | Time | Rate | vs Python |
|---|---|---|---|
| **C# AOT** (1.4 MB binary) | 48ms | **2,083,333 ff/s** | **14.4x** |
| **C# JIT** (.NET 10) | 111ms | **900,900 ff/s** | **6.2x** |
| **Python 3.14t** | 692ms | 144,583 ff/s | baseline |

### Queue Operations (100K)

| Operation | C# AOT | C# JIT | Python 3.14t |
|---|---|---|---|
| Offer | 46ms (2.17M ops/s) | 98ms (1.02M ops/s) | 222ms (450K ops/s) |
| Claim+Ack | 6ms (16.7M ops/s) | 6ms (16.7M ops/s) | 114ms (878K ops/s) |

### GC Pressure

| Metric | Before pooling | After pooling |
|---|---|---|
| Gen0 collections | 21-27 | 15-16 |
| Gen1 collections | 10-13 | 7 |
| Gen2 collections | 5-6 | 4 |
| Session 100K rate | 265-552K ff/s | 900K-2.08M ff/s |

### Binary Size (Native AOT)

```
1.4 MB  — stripped ELF x86-64, statically linked runtime
         IlcOptimizationPreference=Size, InvariantGlobalization,
         no stack traces, no resource strings, trimmed metadata
```

## Key Design Decisions

1. **ThreadStatic over ConcurrentBag** — ConcurrentBag uses per-thread lists with cross-thread
   stealing and CAS operations. For single-threaded hot paths (which is how flow processors run),
   a simple `[ThreadStatic]` array is 10-50x faster for rent/return.

2. **Overlay chain over FrozenDictionary** — FrozenDictionary is immutable but requires full
   construction upfront. The overlay chain amortizes the cost: `With()` is O(1), lookup is O(depth).
   For typical 2-4 hop pipelines, the chain depth is tiny and cache-friendly.

3. **In-place mutation for Claim** — The QueueEntry moves from `_items[]` → `_invisible` dict.
   Since no other code holds a reference to it mid-transition (we're inside the lock), mutating
   `ClaimedAt` in-place is safe and eliminates one object allocation per dequeue.

4. **Server GC** — Enabled via `<ServerGarbageCollection>true</ServerGarbageCollection>`.
   Uses dedicated GC threads and larger generation sizes, which is better for throughput
   workloads (longer between collections, larger Gen0 budget).

5. **Native AOT + Size trimming** — AOT eliminates JIT warmup and produces a 1.4MB self-contained
   binary. The `[ThreadStatic]` pool works identically under AOT since it's a runtime feature,
   not a JIT optimization.
