# Zinc Flow — Testing Strategy

> Research and planning document for comprehensive testing of zinc-flow, using Apache NiFi as the comparison benchmark.

## Goals

1. **Correctness** — zinc-flow processes data correctly under all conditions
2. **Performance parity** — competitive with NiFi on equivalent workloads
3. **Fault tolerance** — graceful degradation, no data loss, no silent failures
4. **Actor model validation** — shutdown, kill, restart, supervision all work as designed
5. **Comparison baseline** — quantified NiFi vs zinc-flow on identical workloads

---

## 1. Unit Tests — Processor Level

Equivalent to NiFi's `TestRunner` framework. Test individual processors in isolation.

### What to test
- **Processor correctness**: given input FlowFile, verify output FlowFile attributes/content
- **Error handling**: processor throws → verify error propagation, no data loss
- **Sealed result types**: Single, Multiple, Routed, Drop — exhaustive testing of each return path
- **Edge cases**: empty content, null attributes, very large content (100MB+), malformed data

### Approach
```zinc
// Test helper — create a FlowFile, send to processor, verify result
fn testProcessor(ProcessorFn proc, FlowFile input, ProcessorResult expected) {
    var result = proc.process(input)
    assert result == expected
}
```

### NiFi comparison
NiFi uses `TestRunner` + `MockFlowFile`. We should build an equivalent `FlowTestRunner`:
- `runner.enqueue(content, attributes)` — add input FlowFile
- `runner.run()` — process all queued FlowFiles
- `runner.getOutputFlowFiles()` — get results
- `runner.assertQueueEmpty()` — verify all input consumed

---

## 2. Actor Tests — Lifecycle & Concurrency

Test the actor model itself — message delivery, ordering, shutdown, kill, supervision.

### What to test
- **Message ordering**: messages processed in FIFO order within a single actor
- **Fire-and-forget delivery**: all enqueued messages are eventually processed
- **Request-reply correctness**: correct value returned, no deadlocks
- **Shutdown drain**: pending messages processed before actor exits
- **Shutdown timeout**: actor interrupted after timeout, no hang
- **Kill**: actor stops immediately, pending messages discarded
- **Kill reaper**: ActorRuntime.pendingKill → thread dies → removed from queue
- **Kill reaper escalation**: thread refuses to die → System.exit(1) (test in isolated process)
- **Concurrent senders**: multiple threads sending to same actor — no lost messages, no corruption
- **Actor isolation**: modifying state in one actor doesn't affect another

### Approach
```zinc
// Verify message ordering
actor OrderTracker {
    var List<int> received = []
    receive fn add(int n) { received.add(n) }
    receive fn getAll(): List<int> { return received }
}

var tracker = new OrderTracker()
for i in 0..100 { tracker.add(i) }
Thread.sleep(200)
var all = tracker.getAll()
assert all == [0, 1, 2, ..., 99]  // FIFO guaranteed
```

---

## 3. Pipeline Integration Tests

Test complete pipelines — multi-stage processing, wiring, data flow.

### What to test
- **Linear pipeline**: source → proc1 → proc2 → sink, verify end-to-end
- **Branching/routing**: Routed result → different downstream actors
- **Pipeline start/stop**: clean startup, clean shutdown, no orphaned actors
- **Pipeline kill**: all actors killed, reaper monitors all threads
- **Data integrity**: FlowFile content unchanged through pass-through processors
- **Attribute accumulation**: each processor adds attributes, final FlowFile has all
- **Provenance tracking**: each step recorded in provenance list

### NiFi comparison scenarios
Run identical workloads through both NiFi and zinc-flow, compare:

| Scenario | NiFi Pipeline | zinc-flow Pipeline |
|---|---|---|
| **Simple transform** | GenerateFlowFile → UpdateAttribute → PutFile | HttpSource → addAttribute → FileSink |
| **Content routing** | RouteOnAttribute → 2 branches | Routed result → different actors |
| **Error handling** | RetryFlowFile + dead letter | ProcessorWorker retry + DLQ actor |
| **Fan-out** | SplitText → multiple FlowFiles | Multiple result → multiple messages |
| **Large payload** | 10MB files through pipeline | 10MB FlowFiles through actor chain |

---

## 4. Performance Benchmarks — NiFi vs zinc-flow

Quantified comparison on identical hardware, identical workloads.

### Metrics
- **Throughput**: FlowFiles/second at steady state
- **Latency**: p50, p95, p99 end-to-end (ingestion to sink)
- **Startup time**: cold start to first FlowFile processed
- **Memory footprint**: RSS at idle, RSS under load, RSS at peak
- **CPU utilization**: cores used at steady state throughput
- **Queue depth**: backpressure behavior under overload

### Benchmark workloads

#### A. Throughput — small messages
- 1KB JSON messages, simple attribute transformation
- Target: sustained ingestion rate, measure FlowFiles/sec
- NiFi baseline: ~50K-100K msg/sec on commodity hardware
- Expected zinc-flow advantage: lower overhead (no NAR, no UI, no provenance DB)

#### B. Throughput — large payloads
- 1MB-10MB payloads, pass-through with attribute enrichment
- Measures: bytes/sec, GC pressure, memory allocation rate
- NiFi baseline: ~50-100MB/sec on modest hardware

#### C. Latency — single message
- Send one message, measure time to reach sink
- NiFi: typically 5-50ms depending on pipeline depth
- zinc-flow target: <1ms for actor-to-actor forwarding

#### D. Startup time
- Cold start to ready-for-traffic
- NiFi: 15-45 seconds (JVM warmup + framework init)
- zinc-flow JVM: target <2 seconds
- zinc-flow native-image: target <100ms

#### E. Memory footprint
- Idle pipeline with no traffic
- NiFi: 512MB-1GB minimum heap
- zinc-flow target: <50MB idle

#### F. Scaling — concurrent producers
- N concurrent HTTP clients posting simultaneously
- Measure: throughput scales linearly? Where does it plateau?
- Compare NiFi thread pool model vs zinc-flow actor mailbox model

### Benchmark harness
```bash
# Generate load
wrk -t4 -c100 -d30s -s post.lua http://localhost:8080/flow

# Or use hey/vegeta for precise latency histograms
echo "POST http://localhost:8080/flow" | vegeta attack -body payload.json -rate 10000 -duration 30s | vegeta report
```

---

## 5. Fault Tolerance Tests

Verify zinc-flow handles failures gracefully — no data loss, no silent corruption.

### What to test

#### A. Processor failure
- Processor throws exception → FlowFile goes to DLQ (when wired)
- Processor throws exception → error logged, actor continues processing next message
- Processor hangs (infinite loop) → actor kill works, pipeline continues

#### B. Backpressure
- Producer faster than consumer → actor mailbox grows, producer blocks (bounded mailbox)
- Verify: no OOM, no dropped messages, producer slows naturally
- Compare: NiFi backpressure (queue threshold) vs actor mailbox backpressure

#### C. Actor death and restart
- Actor crashes mid-processing → supervisor detects, restarts
- Verify: messages in-flight at crash time are handled (at-least-once or at-most-once)
- Document: what's the delivery guarantee? Currently at-most-once (message in mailbox is lost on kill)

#### D. Resource exhaustion
- Disk full → FileSink fails → verify error propagation, no infinite retry
- Memory pressure → GC pressure → verify no data corruption under GC pauses
- File descriptor exhaustion → HTTP source stops accepting, recovers when FDs free

#### E. Chaos testing scenarios
- Kill random actor mid-pipeline → verify pipeline recovers
- Kill supervisor → verify all children die
- Network partition (for future distributed mode) → verify queue-bridged groups handle it
- Process crash → verify clean restart (no corrupt state files)

---

## 6. Hot Swap / Live Replacement Tests

Verify processors can be replaced at runtime without data loss (Phase 2 feature, design tests now).

### What to test
- Stop processor → queue buffers → start new processor → queue drains
- Zero messages lost during swap window
- Swap under load — continuous traffic during replacement
- Rollback — new processor fails, revert to old version
- Version tracking — pipeline records which processor version processed each FlowFile

### NiFi comparison
NiFi supports processor stop/start via UI/API. Blue-green deployment for zero-downtime swaps. zinc-flow's actor model enables: shutdown actor → create new actor with new ProcessorFn → wire into chain.

---

## 7. End-to-End Scenario Tests

Real-world workflows that exercise the full system.

### Scenario A: Log ingestion
- HTTP source receives JSON log lines
- Processor 1: parse JSON, extract fields to attributes
- Processor 2: route by log level (error → alert sink, info → archive sink)
- Verify: correct routing, no data loss, handles malformed JSON

### Scenario B: File processing
- Directory watcher source (future) picks up CSV files
- Processor 1: split into individual records
- Processor 2: transform/enrich each record
- Processor 3: batch and write to output directory
- Verify: all records processed, output matches expected, handles large files

### Scenario C: API enrichment
- HTTP source receives order events
- Processor 1: call external API to enrich (mock service)
- Processor 2: validate enriched data
- Processor 3: route valid → output, invalid → DLQ
- Verify: handles API timeouts, retries work, DLQ captures failures

### Scenario D: High-volume sustained load
- 1 million messages over 10 minutes
- Verify: zero message loss, stable memory, no degradation over time
- Compare: NiFi provenance overhead vs zinc-flow lightweight tracking

---

## 8. Comparison Test Infrastructure

### NiFi reference setup
- NiFi 2.x running locally or in Docker
- Equivalent pipeline configured via NiFi API (NiPyApi or REST)
- Same hardware, same JVM settings where applicable
- Automated: script creates NiFi flow, runs benchmark, collects metrics

### zinc-flow test setup
- zinc-flow running from `zinc run`
- Same pipeline topology as NiFi
- Same load generation tool (wrk, vegeta, or hey)
- Metrics collected: throughput, latency histogram, memory, CPU

### Results format
```
| Metric              | NiFi 2.x    | zinc-flow   | Ratio  |
|---------------------|-------------|-------------|--------|
| Throughput (msg/s)  | 50,000      | ???         | ???    |
| p50 latency (ms)    | 12          | ???         | ???    |
| p99 latency (ms)    | 45          | ???         | ???    |
| Startup time (s)    | 25          | ???         | ???    |
| Idle memory (MB)    | 768         | ???         | ???    |
| Peak memory (MB)    | 2048        | ???         | ???    |
```

---

## 9. Test Automation & CI

### Test tiers
1. **Tier 1 — Unit** (run on every commit, <10 seconds)
   - Processor unit tests
   - Actor lifecycle tests
   - Codegen string-match tests (existing)

2. **Tier 2 — Integration** (run on every PR, <2 minutes)
   - Pipeline e2e tests
   - Multi-file build tests
   - HTTP source → actor chain → file sink

3. **Tier 3 — Performance** (nightly or on-demand, ~30 minutes)
   - Benchmark suite against NiFi
   - Sustained load tests
   - Memory/CPU profiling

4. **Tier 4 — Chaos** (weekly or pre-release, ~1 hour)
   - Fault injection
   - Kill/restart scenarios
   - Resource exhaustion

### Test framework
- **Zinc test files**: `.zn` test scripts with expected output (existing pattern from `examples/v3/`)
- **Go test harness**: for codegen and transpiler tests (existing)
- **Shell scripts**: for e2e and benchmark orchestration
- **Docker Compose**: for NiFi comparison environment (NiFi + zinc-flow + load generator)

---

## 10. Open Questions

- **Delivery guarantees**: Current actor model is at-most-once (kill discards mailbox). Should we support at-least-once? Requires checkpoint/ack mechanism.
- **Bounded mailbox**: Actor mailbox is currently unbounded (LinkedBlockingQueue). Should we add configurable bounds for backpressure? ArrayBlockingQueue with capacity would block producers.
- **Provenance overhead**: NiFi's provenance tracking adds significant overhead. zinc-flow's lightweight provenance (list of strings on FlowFile) — how does it compare?
- **Content repository**: For large payloads, should FlowFiles use content references (like NiFi) instead of inline byte[]? At what size threshold?
- **Distributed mode testing**: Cross-group NATS messaging (Phase 2) — needs network partition and split-brain tests.

---

## Sources

- [NiFi Performance Expectations](https://hdpweb.o.onslip.net/HDPDocuments/HDF3/HDF-3.4.0/apache-nifi-overview/content/performance-expectations-and-characteristics-of-nifi.html)
- [Processing One Billion Events/sec with NiFi](https://www.cloudera.com/blog/technical/benchmarking-nifi-performance-and-scalability.html)
- [NiFi TestRunner Framework](https://github.com/apache/nifi/blob/master/nifi-mock/src/main/java/org/apache/nifi/util/TestRunner.java)
- [NiFi Developer's Guide — Testing](https://nifi.apache.org/docs/nifi-docs/html/developer-guide.html)
- [Chaos Testing Best Practices](https://www.apriorit.com/qa-blog/chaos-testing-best-practices)
- [NiFi Thread Configuration Optimization](https://medium.com/@scorpy257/demystifying-apache-nifi-thread-configurations-optimizing-performance-and-throughput-af6f2b455a6b)
- [NiFi Flow Testing with NiPyApi](https://medium.com/@dharmachand/comprehensive-guide-to-nifi-flow-testing-with-nipyapi-e44a61975be9)
