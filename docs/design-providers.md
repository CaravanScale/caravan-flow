# Provider Architecture Design

## Context

ServiceProvider currently only holds ContentStore. Before building connectors, we need a proper service layer. Every major framework has this:

- **NiFi**: Controller Services (named, lifecycle-managed, referenced by processors)
- **Flink**: RuntimeContext (injected in `open()`, provides state stores, config)
- **Pulsar**: Context object (`getSecret()`, `getUserConfigMap()`, state, metrics)
- **Spring Cloud Stream**: Binder abstraction + Spring DI for connections
- **Beam**: Setup/Teardown lifecycle + PipelineOptions for config

Common patterns across all:
1. **Single context object** — one place for config, providers, secrets, metrics
2. **Named provider registry** — processors reference shared resources by name
3. **Lifecycle hooks** — enable/disable for resource management
4. **Config injection** — external config (env vars, K8s, files), not hardcoded
5. **Secrets separation** — credentials handled separately, never logged

## Design: ProcessorContext + Providers

### Provider Interface

Everything is a provider — content storage, NATS connections, SSL contexts, credential stores. No special cases.

```zinc
interface Provider {
    fn getName(): String
    fn getType(): String
    fn enable()
    fn disable()
    fn isEnabled(): bool
}
```

Specific providers extend this with their own capabilities:

```zinc
class FileContentProvider : Provider {
    // provides store(data): claimId, retrieve(claimId): bytes, etc.
}

class MemoryContentProvider : Provider {
    // same interface, in-memory for testing
}

class NatsProvider : Provider {
    // provides getConnection(): nats.Conn
}

class SSLProvider : Provider {
    // provides getSSLContext(): tls.Config
}
```

ContentStore becomes a provider type like any other. No special field, no special accessor.

### ProcessorContext

Processors receive a **ProcessorContext** at creation time. Single access point for everything:

```zinc
class ProcessorContext {
    ServiceProvider services
    Map<String, String> processorConfig   // this processor's config
    String processorName                   // this processor's name

    init(ServiceProvider services, Map<String, String> processorConfig, String processorName) {
        this.services = services
        this.processorConfig = processorConfig
        this.processorName = processorName
    }

    // Config for this processor
    pub fn getConfig(String key): String
    pub fn getConfigOrDefault(String key, String defaultValue): String

    // Provider lookup — returns error if not found
    pub fn getProvider(String name): Provider or Error

    // Required config — returns error if key missing
    pub fn requireConfig(String key): String or Error

    // Credentials — returns error if not found
    pub fn getCredential(String name): String or Error

    // Logging (named logger for this processor)
    pub fn log(): Logger
}
```

No `getContentStore()` shortcut. Processors that need content storage look up their provider by name, using `or` for error handling:

```zinc
fn FileSinkFactory(ProcessorContext ctx): ProcessorFn {
    var contentProvider = ctx.getProvider(ctx.requireConfig("content-provider") or { return error }) or { return error }
    var outputDir = ctx.getConfig("output_dir")
    return FileSink(outputDir, contentProvider)
}
```

### ProcessorFn Change

Processor factories return `(ProcessorFn, Error)` — creation can fail if required providers or config are missing:

```zinc
// Current:
type ProcessorFactory = Fn<(Map<String, String>, ServiceProvider), ProcessorFn>

// New:
type ProcessorFactory = Fn<(ProcessorContext), ProcessorFn or Error>
```

Fabric uses `or` at flow loading time — missing provider or config is fatal:
```zinc
var proc = reg.create(typeName, context) or {
    logging.error("failed to create processor", "name", name, "error", err)
    panic("zinc-flow cannot start: ${err}")
}
```

**Two failure modes:**

1. **Startup failures** (missing provider, bad config, provider won't enable) — zinc-flow refuses to start. All or nothing. These are configuration errors.

2. **Data flow failures** (processor can't handle a FlowFile) — FlowFile routes to DLQ via `Failure(reason, ff)`. System keeps running. These are runtime data errors handled by the routing engine.

Provider/config errors are always category 1. The flow either starts fully configured or doesn't start at all.

### ServiceProvider Redesign

ServiceProvider is now just the backing store — provider registry + config + credentials:

```zinc
class ServiceProvider {
    var providers = Map<String, Provider>{}
    var config = Map<String, String>{}
    var credentials = Map<String, String>{}

    init() {}

    // Providers — error if not found
    pub fn getProvider(String name): Provider or Error
    pub fn addProvider(Provider provider)
    pub fn removeProvider(String name)
    pub fn listProviders(): List<Provider>
    pub fn enableAll()
    pub fn disableAll()

    // Config (flat key-value, loaded from YAML + env)
    pub fn getConfig(String key): String
    pub fn getConfigOrDefault(String key, String defaultValue): String
    pub fn setConfig(String key, String value)

    // Credentials — error if not found
    pub fn getCredential(String name): String or Error
    pub fn setCredential(String name, String value)
}
```

No ContentStore field. No special constructor. Just a registry. Lookups that can fail return errors — callers use `or` to handle.

### Provider Registry

Maps provider type names to factory functions (like ProcessorRegistry):

```zinc
type ProviderFactory = Fn<(String, Map<String, String>), Provider>

class ProviderRegistry {
    var factories = Map<String, ProviderFactory>{}

    pub fn register(String typeName, ProviderFactory factory)
    pub fn create(String typeName, String name, Map<String, String> config): Provider
    pub fn has(String typeName): bool
    pub fn list(): List<String>
}
```

### Config Loading

```yaml
# config.yaml
providers:
  - name: content
    type: file-content
    config:
      dir: /tmp/zinc-flow/content

  - name: nats-main
    type: nats
    config:
      url: nats://localhost:4222

flow:
  processors:
    - name: tag-env
      type: add-attribute
      config:
        key: env
        value: prod
    - name: sink
      type: file-sink
      config:
        output_dir: /tmp/zinc-flow/output
        content-provider: content    # references provider by name
    - name: nats-out
      type: put-nats
      config:
        provider: nats-main
        subject: orders.tagged
```

Config hierarchy (highest priority wins):
1. Environment variables: `ZINC_FLOW_NATS_URL` -> `nats.url`
2. K8s ConfigMap/Secrets (mounted as env vars)
3. config.yaml values
4. Hardcoded defaults

### How Processors Use Providers

All lookups use `or` — missing provider/config returns error, which propagates up to Fabric and stops startup.

```zinc
// FileSink uses content provider for resolving claims
fn FileSinkFactory(ProcessorContext ctx): ProcessorFn {
    var contentProvider = ctx.getProvider("content") or { return error }
    var outputDir = ctx.requireConfig("output_dir") or { return error }
    return FileSink(outputDir, contentProvider)
}

// PutNats uses NATS provider for shared connection
fn PutNatsFactory(ProcessorContext ctx): ProcessorFn or Error {
    var natsProvider = ctx.getProvider(ctx.getConfig("provider"))
    var subject = ctx.getConfig("subject")
    return PutNats(natsProvider, subject)
}
```

### Management API

```
GET  /api/providers           — list providers with status
POST /api/providers/add       — create and register a provider
DELETE /api/providers/remove   — disable and remove a provider
PUT  /api/providers/toggle    — enable/disable a provider
GET  /api/config              — view runtime config (credentials redacted)
PUT  /api/config              — update a runtime config value
```

## Implementation Order

1. Provider interface
2. FileContentProvider + MemoryContentProvider (wrap existing ContentStore logic)
3. ProcessorContext class
4. Redesign ServiceProvider — provider registry + config + credentials, no ContentStore field
5. ProviderRegistry
6. Update ProcessorFactory type signature
7. Update all 5 builtin processors to use ProcessorContext
8. Update Fabric — load providers from config, create ProcessorContext per processor
9. Config hierarchy — env vars override YAML
10. API endpoints — provider management + config view
11. Update tests
12. NatsProvider — first external provider (after framework is solid)
