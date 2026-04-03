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

### ProcessorContext

Instead of ServiceProvider growing into a god object, processors receive a **ProcessorContext** at creation time. This is the single access point for everything a processor needs:

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

    // Provider lookup
    pub fn getProvider(String name): Provider

    // Content store (convenience — delegates to services)
    pub fn getContentStore(): ContentStore

    // Credentials (convenience — delegates to services)
    pub fn getCredential(String name): String

    // Logging (named logger for this processor)
    pub fn log(): Logger
}
```

### ProcessorFn Change

Processor factories receive ProcessorContext instead of raw config + ServiceProvider:

```zinc
// Current:
type ProcessorFactory = Fn<(Map<String, String>, ServiceProvider), ProcessorFn>

// New:
type ProcessorFactory = Fn<(ProcessorContext), ProcessorFn>
```

This is a breaking change but a clean one — every factory gets one object instead of two.

### ServiceProvider Redesign

ServiceProvider becomes the backing store for ProcessorContext:

```zinc
class ServiceProvider {
    ContentStore contentStore
    var providers = Map<String, Provider>{}
    var config = Map<String, String>{}
    var credentials = Map<String, String>{}

    init(ContentStore contentStore) { ... }

    // Content store
    pub fn getContentStore(): ContentStore

    // Providers
    pub fn getProvider(String name): Provider
    pub fn addProvider(Provider provider)
    pub fn removeProvider(String name)
    pub fn listProviders(): List<Provider>
    pub fn enableAll()
    pub fn disableAll()

    // Config (flat key-value, loaded from YAML + env)
    pub fn getConfig(String key): String
    pub fn setConfig(String key, String value)

    // Credentials (separate, never logged)
    pub fn getCredential(String name): String
    pub fn setCredential(String name, String value)
}
```

### Provider Interface

```zinc
interface Provider {
    fn getName(): String
    fn getType(): String
    fn enable()
    fn disable()
    fn isEnabled(): bool
}
```

Specific providers extend this:

```zinc
class NatsProvider : Provider {
    // provides getConnection(): nats.Conn
}

class SSLProvider : Provider {
    // provides getSSLContext(): tls.Config
}
```

### Provider Registry

Like ProcessorRegistry — maps provider type names to factory functions:

```zinc
type ProviderFactory = Fn<(Map<String, String>), Provider>

class ProviderRegistry {
    var factories = Map<String, ProviderFactory>{}

    pub fn register(String typeName, ProviderFactory factory)
    pub fn create(String typeName, Map<String, String> config): Provider
    pub fn has(String typeName): bool
    pub fn list(): List<String>
}
```

### Config Loading

```yaml
# config.yaml
providers:
  - name: nats-main
    type: nats
    config:
      url: nats://localhost:4222

  - name: content
    type: file-content
    config:
      dir: /tmp/zinc-flow/content

flow:
  processors:
    - name: nats-out
      type: put-nats
      config:
        provider: nats-main        # references provider by name
        subject: orders.tagged
```

Config hierarchy (highest priority wins):
1. Environment variables: `ZINC_FLOW_NATS_URL` -> `nats.url`
2. K8s ConfigMap/Secrets (mounted as env vars)
3. config.yaml values
4. Hardcoded defaults

### How a Connector Processor Uses This

```zinc
class PutNats : ProcessorFn {
    NatsProvider natsProvider
    String subject
    ContentStore store

    init(NatsProvider natsProvider, String subject, ContentStore store) {
        this.natsProvider = natsProvider
        this.subject = subject
        this.store = store
    }

    pub fn process(FlowFile ff): ProcessorResult {
        var conn = natsProvider.getConnection()
        // ... serialize and publish
    }
}

fn PutNatsFactory(ProcessorContext ctx): ProcessorFn {
    var natsProvider = ctx.getProvider(ctx.getConfig("provider"))
    var subject = ctx.getConfig("subject")
    return PutNats(natsProvider, subject, ctx.getContentStore())
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
2. ProcessorContext class
3. Redesign ServiceProvider — add provider registry, config, credentials
4. ProviderRegistry
5. Update ProcessorFactory type signature
6. Update all 5 builtin processors to use ProcessorContext
7. Update Fabric — create ProcessorContext per processor, load providers from config
8. Config hierarchy — env vars override YAML
9. API endpoints — provider management + config view
10. Update tests
11. NatsProvider — first real provider (after framework is solid)
