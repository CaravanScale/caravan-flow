# Zinc Flow — Architecture (Zinc Pseudocode)

> Complete architecture of zinc-flow expressed in zinc pseudocode.
> All constructs below use features that zinc-go already supports
> (data classes, interfaces, classes, inheritance, sealed classes,
> enums, typed channels, spawn, match, error handling).

---

## 1. Core Types

### FlowFile — The Unit of Data

```zinc
data FlowFile(
    String id,
    Map<String, String> attributes,
    String content,
    int timestamp
)
```

Helper functions (free functions, not methods on data — zinc-go data classes are plain structs):

```zinc
fn withAttribute(FlowFile ff, String key, String value): FlowFile {
    var attrs = ff.attributes
    attrs[key] = value
    return FlowFile(ff.id, attrs, ff.content, int(time.Now().Unix()))
}

fn withContent(FlowFile ff, String newContent): FlowFile {
    return FlowFile(ff.id, ff.attributes, newContent, int(time.Now().Unix()))
}

fn newFlowFile(String content): FlowFile {
    return FlowFile(uuid(), {}, content, int(time.Now().Unix()))
}
```

### ProcessorResult — What Processors Return

```zinc
sealed class ProcessorResult {
    data Single(FlowFile ff)
    data Multiple(List<FlowFile> ffs)
    data Routed(String route, FlowFile ff)
    data object Drop
}
```

### ProcessorFn — The Processor Interface

```zinc
interface ProcessorFn {
    fn process(FlowFile ff): FlowFile
}
```

> Phase 1 uses `FlowFile -> FlowFile` for simplicity (matching the current `main.zn`).
> Phase 2 upgrades to `FlowFile -> ProcessorResult` once sealed class match is validated.

---

## 2. Built-in Processors

### AddAttribute — Tag FlowFiles with Metadata

```zinc
class AddAttribute : ProcessorFn {
    String key
    String value

    init(String key, String value) {
        this.key = key
        this.value = value
    }

    pub fn process(FlowFile ff): FlowFile {
        var tagged = ff.content + "\n[" + key + "=" + value + "]"
        return FlowFile(ff.id, ff.attributes, tagged, ff.timestamp)
    }
}
```

### FileSink — Write FlowFiles to Disk

```zinc
class FileSink : ProcessorFn {
    String outputDir

    init(String outputDir) {
        this.outputDir = outputDir
    }

    pub fn process(FlowFile ff): FlowFile {
        var path = outputDir + "/" + ff.id + ".out"
        os.WriteFile(path, ff.content.toBytes(), 0644)
        return ff
    }
}
```

### LogProcessor — Print FlowFile to Stdout

```zinc
class LogProcessor : ProcessorFn {
    String prefix

    init(String prefix) {
        this.prefix = prefix
    }

    pub fn process(FlowFile ff): FlowFile {
        print("{prefix}: id={ff.id} content={ff.content}")
        return ff
    }
}
```

### TransformContent — Apply a String Transform

```zinc
class TransformContent : ProcessorFn {
    String prefix
    String suffix

    init(String prefix, String suffix) {
        this.prefix = prefix
        this.suffix = suffix
    }

    pub fn process(FlowFile ff): FlowFile {
        var newContent = prefix + ff.content + suffix
        return FlowFile(ff.id, ff.attributes, newContent, ff.timestamp)
    }
}
```

---

## 3. ProcessorWorker — Goroutine Worker Loop

Each processor gets a worker that runs as a goroutine, consuming from an input channel and pushing to an output channel.

```zinc
class ProcessorWorker {
    String name
    ProcessorFn fn
    var input = Channel<FlowFile>(1024)
    var output = Channel<FlowFile>(1024)
    var running = false
    var processed = 0
    var errors = 0

    init(String name, ProcessorFn fn) {
        this.name = name
        this.fn = fn
    }

    pub fn start() {
        running = true
        spawn { runLoop() }
    }

    pub fn stop() {
        running = false
    }

    fn runLoop() {
        while running {
            var ff = input.recv()
            var result = fn.process(ff)
            processed = processed + 1
            output.send(result)
        }
    }

    pub fn stats(): String {
        return "{name}: processed={processed} errors={errors}"
    }
}
```

### Scaled Worker — Multiple Goroutines on One Channel

```zinc
class ScaledWorker {
    String name
    ProcessorFn fn
    var input = Channel<FlowFile>(1024)
    var output = Channel<FlowFile>(1024)
    var replicas = 1
    var running = false

    init(String name, ProcessorFn fn, int replicas) {
        this.name = name
        this.fn = fn
        this.replicas = replicas
    }

    pub fn start() {
        running = true
        for i in 0..replicas {
            spawn { runLoop() }
        }
    }

    pub fn stop() {
        running = false
    }

    fn runLoop() {
        while running {
            var ff = input.recv()
            var result = fn.process(ff)
            output.send(result)
        }
    }
}
```

Multiple goroutines consuming from the same channel gives competing-consumer semantics — Go channels are goroutine-safe. This is how you scale a slow processor.

---

## 4. Pipeline — Wiring Processors Together

```zinc
class Pipeline {
    String name
    var workers = List<ProcessorWorker>[]

    init(String name) {
        this.name = name
    }

    // Add a processor, returns its index
    pub fn add(String name, ProcessorFn fn): int {
        var worker = ProcessorWorker(name, fn)
        workers.add(worker)
        return len(workers) - 1
    }

    // Wire output of processor A to input of processor B
    pub fn connect(int from, int to) {
        // Share the channel: source's output IS target's input
        workers[to].input = workers[from].output
    }

    // Start all workers, block until shutdown
    pub fn run() {
        print("Pipeline '{name}' starting ({len(workers)} processors)")
        for worker in workers {
            worker.start()
        }
    }

    pub fn stop() {
        for worker in workers {
            worker.stop()
        }
    }

    pub fn status() {
        for worker in workers {
            print("  " + worker.stats())
        }
    }
}
```

---

## 5. Sources — Pushing Data Into the Pipeline

### HttpSource — Receive HTTP Requests as FlowFiles

```zinc
import net.http
import fmt

class HttpSource {
    int port
    var output = Channel<FlowFile>(1024)

    init(int port) {
        this.port = port
    }

    pub fn start() {
        http.HandleFunc("/ingest", handleIngest)
        print("HttpSource listening on :{port}")
        spawn {
            http.ListenAndServe(":{port}", nil)
        }
    }

    fn handleIngest(http.ResponseWriter w, http.Request r) {
        // Read request body, create FlowFile, push to output channel
        var ff = FlowFile(
            uuid(),
            {"http.method": "POST", "http.path": "/ingest"},
            "request-body-here",
            int(time.Now().Unix())
        )
        output.send(ff)
        fmt.Fprintf(w, "accepted")
    }
}
```

### ChannelSource — Programmatic Source (for Testing)

```zinc
class ChannelSource {
    var output = Channel<FlowFile>(1024)

    pub fn send(FlowFile ff) {
        output.send(ff)
    }

    pub fn sendMany(List<FlowFile> ffs) {
        for ff in ffs {
            output.send(ff)
        }
    }
}
```

---

## 6. Putting It All Together — Example Pipelines

### Example A: Simple Linear Pipeline (Phase 1 — works now)

```zinc
import os
import fmt

fn main() {
    os.MkdirAll("/tmp/zinc-flow-out", 0755)

    // Typed channels between stages
    var ch1 = Channel<FlowFile>(1024)
    var ch2 = Channel<FlowFile>(1024)
    var ch3 = Channel<FlowFile>(1024)

    // Stage 1: add-attribute processor
    var addAttr = AddAttribute("processed", "true")
    spawn {
        while true {
            var ff = ch1.recv()
            var result = addAttr.process(ff)
            ch2.send(result)
        }
    }

    // Stage 2: file-sink processor
    var sink = FileSink("/tmp/zinc-flow-out")
    spawn {
        while true {
            var ff = ch2.recv()
            var result = sink.process(ff)
            ch3.send(result)
        }
    }

    // Send test data
    print("Pipeline started")
    for i in 1..4 {
        var id = "ff-" + str(i)
        var ff = FlowFile(id, "hello zinc flow " + str(i), 0)
        ch1.send(ff)
    }

    // Collect results
    for i in 1..4 {
        var done = ch3.recv()
        print("  done: {done}")
    }
    print("All FlowFiles processed")
}
```

### Example B: Pipeline with ProcessorWorker (Phase 1 — next step)

```zinc
fn main() {
    os.MkdirAll("/tmp/zinc-flow-out", 0755)

    var pipeline = Pipeline("demo")

    var i0 = pipeline.add("add-attr", AddAttribute("source", "demo"))
    var i1 = pipeline.add("transform", TransformContent("[", "]"))
    var i2 = pipeline.add("log", LogProcessor("output"))
    var i3 = pipeline.add("sink", FileSink("/tmp/zinc-flow-out"))

    pipeline.connect(i0, i1)
    pipeline.connect(i1, i2)
    pipeline.connect(i2, i3)

    pipeline.run()

    // Feed data into first worker's input channel
    var input = pipeline.workers[0].input
    for i in 1..11 {
        input.send(FlowFile("ff-" + str(i), {}, "message " + str(i), 0))
    }

    // Give time to process, then print stats
    time.Sleep(100 * time.Millisecond)
    pipeline.status()
    pipeline.stop()
}
```

### Example C: HTTP Source -> Process -> Sink (Phase 1 target)

```zinc
fn main() {
    os.MkdirAll("/tmp/zinc-flow-out", 0755)

    // Source: HTTP listener
    var source = HttpSource(8080)
    source.start()

    // Processors
    var addAttr = AddAttribute("received_by", "zinc-flow")
    var sink = FileSink("/tmp/zinc-flow-out")

    // Wire: source.output -> addAttr -> sink
    spawn {
        while true {
            var ff = source.output.recv()
            var tagged = addAttr.process(ff)
            sink.process(tagged)
        }
    }

    print("zinc-flow listening on :8080/ingest")
    print("POST a message: curl -X POST -d 'hello' http://localhost:8080/ingest")

    // Block forever (Ctrl+C to stop)
    select {}
}
```

### Example D: Worker Pool — Scaled Processor (Phase 1)

```zinc
fn main() {
    var input = Channel<FlowFile>(100)
    var output = Channel<FlowFile>(100)

    // 5 worker goroutines consuming from same channel
    var processor = AddAttribute("worker", "true")
    for w in 0..5 {
        spawn {
            while true {
                var ff = input.recv()
                var result = processor.process(ff)
                output.send(result)
            }
        }
    }

    // Send 100 FlowFiles
    for i in 0..100 {
        input.send(FlowFile("ff-" + str(i), {}, "msg-" + str(i), 0))
    }

    // Collect all results
    for i in 0..100 {
        var ff = output.recv()
    }
    print("100 FlowFiles processed by 5 workers")
}
```

---

## 7. Phase 2 Extensions (Pseudocode)

### ProcessorResult with Match (requires sealed class match in zinc-go)

```zinc
interface ProcessorFnV2 {
    fn process(FlowFile ff): ProcessorResult
}

class Router {
    ProcessorFnV2 fn
    var outputs = Map<String, Channel<FlowFile>>{}

    fn route(ProcessorResult result) {
        match result {
            case Single(ff) {
                outputs.get("default").send(ff)
            }
            case Multiple(ffs) {
                for ff in ffs {
                    outputs.get("default").send(ff)
                }
            }
            case Routed(name, ff) {
                outputs.get(name).send(ff)
            }
            case Drop {
                // discard
            }
        }
    }
}
```

### NATS Cross-Group Queue (Phase 2)

```zinc
class NatsQueue {
    // Uses nats-io/nats.go
    // Serialize FlowFile to JSON at group boundary
    // JetStream for durability
    // Consumer groups for competing consumers

    pub fn put(FlowFile ff) {
        var data = json.Marshal(ff)
        js.Publish(subject, data)
    }

    pub fn poll(): FlowFile {
        var msg = sub.NextMsg(timeout)
        var ff = json.Unmarshal(msg.Data)
        msg.Ack()
        return ff
    }
}
```

### Management API (Phase 2)

```zinc
class FlowAPI {
    Pipeline pipeline

    pub fn start(int port) {
        http.HandleFunc("/api/status", handleStatus)
        http.HandleFunc("/api/processors", handleProcessors)
        http.HandleFunc("/api/health", handleHealth)
        http.ListenAndServe(":{port}", nil)
    }

    fn handleStatus(http.ResponseWriter w, http.Request r) {
        pipeline.status()
        fmt.Fprintf(w, "ok")
    }

    fn handleHealth(http.ResponseWriter w, http.Request r) {
        fmt.Fprintf(w, "healthy")
    }
}
```

---

## 8. Architecture Diagram

```
                    zinc-flow (single process, local dev)
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  ┌──────────┐   Channel   ┌──────────┐   Channel   ┌─────┐ │
│  │ HTTP     │──────────-->│ AddAttr  │──────────-->│File │ │
│  │ Source   │  <FlowFile> │ Worker   │  <FlowFile> │Sink │ │
│  │ :8080    │   (1024)    │ goroutine│   (1024)    │     │ │
│  └──────────┘             └──────────┘             └─────┘ │
│                                                              │
│  Pipeline.run() starts all workers as goroutines             │
│  Back-pressure: channel full → sender blocks                 │
│  Scaling: N goroutines on same input channel                 │
└──────────────────────────────────────────────────────────────┘

                    zinc-flow (distributed, Phase 2)
┌─────────────────────┐         ┌─────────────────────┐
│ Pod 1: ingest-group │  NATS   │ Pod 2: enrich-group │
│                     │ JetStream│                     │
│  [HTTP] → [parse]  ─┼────────>│ [enrich] → [sink]  │
│  goroutines+channels│         │ goroutines+channels │
│  1 replica          │         │ 10 replicas         │
└─────────────────────┘         └─────────────────────┘
```

---

## 9. What Zinc-Go Needs (Compiler Gaps to Track)

Features used in this architecture and their zinc-go status:

| Feature | Status | Used For |
|---|---|---|
| `data FlowFile(...)` | ✅ works | FlowFile type |
| `interface ProcessorFn` | ✅ works | Processor contract |
| `class X : Interface` | ✅ works | Processor implementations |
| `Channel<FlowFile>(n)` | ✅ works | Typed bounded channels |
| `spawn { ... }` | ✅ works | Goroutine workers |
| Channel as class field | ✅ works | Worker input/output |
| `http.HandleFunc` | ✅ works | HTTP source + API |
| `sealed class` (declaration) | ✅ works | ProcessorResult |
| `match` on sealed variants | ⚠️ verify | Result routing |
| `select {}` (empty select) | ⚠️ verify | Block forever |
| `Map<String, Channel<FlowFile>>` | ⚠️ verify | Named output routing |
| `os.Getenv` | ⚠️ verify | Secrets provider |
| `time.Since`, `time.Now` | ✅ works | Stats/timing |
| `defer/recover` | ⚠️ verify | Panic recovery in workers |
