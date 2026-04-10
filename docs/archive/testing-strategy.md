# Zinc Flow — Testing Strategy

> Testing strategy for zinc-flow, using Apache NiFi as the comparison benchmark.

## Goals

1. **Correctness** — zinc-flow processes data correctly under all conditions
2. **Performance parity** — competitive with NiFi on equivalent workloads
3. **Fault tolerance** — graceful degradation, no data loss, no silent failures
4. **Goroutine/channel validation** — shutdown, scaling, back-pressure all work as designed
5. **Comparison baseline** — quantified NiFi vs zinc-flow on identical workloads

---

## 1. Unit Tests — Processor Level

Test individual processors in isolation.

### What to test
- **Processor correctness**: given input FlowFile, verify output FlowFile attributes/content
- **ProcessorResult types**: Single, Multiple, Routed, Drop — exhaustive testing of each
- **Error handling**: processor panics → verify recovery, no data loss
- **Edge cases**: empty content, missing attributes, large content

### Approach
```zinc
fn testProcessor(ProcessorFn proc, FlowFile input, ProcessorResult expected) {
    var result = proc.process(input)
    assert(result == expected)
}
```

### Test helper
Build a `FlowTestRunner` for ergonomic testing:
- `runner.enqueue(content, attributes)` — add input FlowFile
- `runner.run()` — process all queued FlowFiles
- `runner.getOutput()` — get results
- `runner.assertEmpty()` — verify all input consumed

---

## 2. Worker Tests — Lifecycle & Concurrency

Test the ProcessorWorker — goroutine management, channel behavior.

### What to test
- **Start/stop**: worker starts consuming, stops cleanly
- **Channel ordering**: FlowFiles processed in FIFO order
- **Back-pressure**: bounded channel blocks sender when full
- **Scaling**: adding goroutines increases throughput
- **Concurrent senders**: multiple goroutines sending to same channel — no lost messages
- **Graceful shutdown**: pending items in channel processed before exit

### Approach
```zinc
fn testWorkerOrdering() {
    var worker = ProcessorWorker("test", PassThrough())
    var output = Channel<FlowFile>(100)
    worker.outputs.put("default", output)
    worker.start()

    for i in 0..100 {
        worker.input.send(FlowFile(str(i), {}, "msg-{i}", 0))
    }

    for i in 0..100 {
        var ff = output.recv()
        assert(ff.id == str(i))
    }
    worker.stop()
}
```

---

## 3. Pipeline Integration Tests

Test complete pipelines — multi-stage processing, wiring, data flow.

### What to test
- **Linear pipeline**: source -> proc1 -> proc2 -> sink, verify end-to-end
- **Branching/routing**: Routed result -> different downstream channels
- **Pipeline start/stop**: clean startup, clean shutdown
- **Data integrity**: FlowFile content unchanged through pass-through processors
- **Attribute accumulation**: each processor adds attributes, final FlowFile has all

### NiFi comparison scenarios

| Scenario | NiFi Pipeline | zinc-flow Pipeline |
|---|---|---|
| Simple transform | GenerateFlowFile -> UpdateAttribute -> PutFile | HttpSource -> AddAttribute -> FileSink |
| Content routing | RouteOnAttribute -> 2 branches | Routed result -> different channels |
| Error handling | RetryFlowFile + dead letter | Worker retry + DLQ channel |
| Fan-out | SplitText -> multiple FlowFiles | Multiple result -> multiple messages |
| Large payload | 10MB files through pipeline | 10MB FlowFiles through channel chain |

---

## 4. Performance Benchmarks — NiFi vs zinc-flow

### Metrics
- **Throughput**: FlowFiles/second at steady state
- **Latency**: p50, p95, p99 end-to-end
- **Startup time**: cold start to first FlowFile processed
- **Memory footprint**: RSS at idle, under load, at peak
- **CPU utilization**: cores used at steady state

### Benchmark workloads

| Workload | Description | NiFi baseline | zinc-flow target |
|---|---|---|---|
| Small messages | 1KB JSON, attribute transform | ~50K-100K msg/sec | Match or exceed |
| Large payloads | 1-10MB, pass-through + enrich | ~50-100MB/sec | Match |
| Single message latency | One message end-to-end | 5-50ms | <1ms channel-to-channel |
| Startup time | Cold start to ready | 15-45 seconds | <100ms |
| Idle memory | No traffic | 512MB-1GB | <20MB |

### Benchmark harness
```bash
# Generate load
echo "POST http://localhost:8080/ingest" | \
    vegeta attack -body payload.json -rate 10000 -duration 30s | \
    vegeta report
```

---

## 5. Fault Tolerance Tests

### Processor failure
- Processor panics -> recovered via defer/recover, worker continues
- Failed FlowFile routed to DLQ channel with error metadata
- Worker continues processing next message

### Back-pressure
- Producer faster than consumer -> channel blocks sender (bounded)
- No OOM, no dropped messages, producer slows naturally

### Resource exhaustion
- Disk full -> FileSink fails -> error propagated, no infinite retry
- Channel full -> sender blocks -> upstream naturally throttled

---

## 6. End-to-End Scenario Tests

### Scenario A: Log ingestion
- HTTP source receives JSON log lines
- Processor 1: parse JSON, extract fields to attributes
- Processor 2: route by log level (error -> alert sink, info -> archive sink)
- Verify: correct routing, no data loss

### Scenario B: File processing
- Directory watcher source picks up CSV files
- Processor 1: split into individual records
- Processor 2: transform/enrich each record
- Processor 3: batch and write to output directory
- Verify: all records processed, output matches expected

### Scenario C: High-volume sustained load
- 1 million messages over 10 minutes
- Verify: zero message loss, stable memory, no degradation

---

## 7. Test Automation & CI

### Test tiers
1. **Tier 1 — Unit** (every commit, <10 seconds)
   - Processor unit tests
   - Worker lifecycle tests

2. **Tier 2 — Integration** (every PR, <2 minutes)
   - Pipeline e2e tests
   - HTTP source -> processor chain -> file sink

3. **Tier 3 — Performance** (nightly, ~30 minutes)
   - Benchmark suite against NiFi
   - Sustained load tests

4. **Tier 4 — Chaos** (weekly, ~1 hour)
   - Fault injection
   - Kill/restart scenarios
   - Resource exhaustion

### Test framework
- **Zinc test files**: `.zn` test scripts with expected output
- **Go test harness**: for compiled output testing
- **Shell scripts**: for e2e and benchmark orchestration
