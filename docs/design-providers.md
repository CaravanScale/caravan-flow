# Provider Architecture Design

## Context

Processors need shared infrastructure: content storage, connections, credentials, config. Every major framework provides this — NiFi (Controller Services), Flink (RuntimeContext), Pulsar (Context), Beam (PipelineOptions).

zinc-flow's approach: two types, one file.

## Design: Provider + ProcessorContext

### Provider

Interface for any shared resource. Named, typed, lifecycle-managed.

```zinc
interface Provider {
    fn getName(): String
    fn getType(): String
    fn enable()
    fn disable()
    fn isEnabled(): bool
}
```

All provider types are compiled into the binary. Config says which to instantiate:

```yaml
providers:
  - name: content
    type: file-content
    config:
      dir: /tmp/zinc-flow/content

  - name: nats-main
    type: nats
    config:
      url: nats://localhost:4222
```

Examples: FileContentProvider, MemoryContentProvider, NatsProvider, SSLProvider.

### ProcessorContext

The central context for zinc-flow. Holds providers, config, credentials. Created once at startup, loaded from config. Processor factories receive a child view with processor-specific config layered on top.

```zinc
class ProcessorContext {
    var providers = Map<String, Provider>{}
    var config = Map<String, String>{}
    var credentials = Map<String, String>{}
    var processorConfig = Map<String, String>{}
    var processorName = ""

    // Provider lookup — error if not found or not enabled
    pub fn getProvider(String name): Provider  // or error

    // Config — processor-specific takes precedence over global
    pub fn getConfig(String key): String       // or error
    pub fn getConfigOrDefault(String key, String defaultValue): String

    // Credentials — error if not found
    pub fn getCredential(String name): String  // or error

    // Create child context for a specific processor
    pub fn forProcessor(String name, Map<String, String> procConfig): ProcessorContext
}
```

### Error Handling

All lookups that can fail return errors via Zinc's `or` pattern:

```zinc
var provider = ctx.getProvider("content") or { return error }
var subject = ctx.getConfig("subject") or { return error }
```

**Two failure modes:**
1. **Startup failures** (missing provider, bad config) — zinc-flow refuses to start. All or nothing.
2. **Data flow failures** (processor can't handle a FlowFile) — routes to DLQ. System keeps running.

Provider/config errors are always startup failures.

### ProcessorFactory Change

```zinc
// Old:
type ProcessorFactory = Fn<(Map<String, String>, ServiceProvider), ProcessorFn>

// New:
type ProcessorFactory = Fn<(ProcessorContext), ProcessorFn>
```

### How Processors Use It

```zinc
fn PutNatsFactory(ProcessorContext ctx): ProcessorFn {
    var natsProvider = ctx.getProvider("nats-main") or { return error }
    var subject = ctx.getConfig("subject") or { return error }
    return PutNats(natsProvider, subject)
}

fn FileSinkFactory(ProcessorContext ctx): ProcessorFn {
    var contentProvider = ctx.getProvider("content") or { return error }
    var outputDir = ctx.getConfig("output_dir") or { return error }
    return FileSink(outputDir, contentProvider)
}
```

### Config Hierarchy

Config values (highest priority wins):
1. Environment variables: `ZINC_FLOW_NATS_URL` -> `nats.url`
2. K8s ConfigMap/Secrets (mounted as env vars)
3. config.yaml values
4. Hardcoded defaults

### Management API

```
GET  /api/providers           — list providers with status
POST /api/providers/add       — create and register a provider
DELETE /api/providers/remove   — disable and remove a provider
PUT  /api/providers/toggle    — enable/disable a provider
GET  /api/config              — view runtime config (credentials redacted)
PUT  /api/config              — update a runtime config value
```

## Implementation

File: `src/core/context.zn`

Steps:
1. Provider interface + ProcessorContext (done)
2. FileContentProvider + MemoryContentProvider (wrap existing ContentStore)
3. Update ProcessorFactory type signature
4. Update all 5 builtin processor factories to use ProcessorContext
5. Update Fabric — load providers from config, create child contexts per processor
6. Config hierarchy — env vars override YAML
7. API endpoints — provider management
8. Update tests
