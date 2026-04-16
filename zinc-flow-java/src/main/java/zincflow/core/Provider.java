package zincflow.core;

/// Shared lifecycle contract for providers, implemented by the built-in
/// ConfigProvider, LoggingProvider, ProvenanceProvider, and
/// ContentProvider. User code can implement to expose custom shared
/// infrastructure (metrics sinks, cache pools, schema registries).
/// Mirrors zinc-flow-csharp's IProvider.
public interface Provider {

    /// Stable name used by processors to look the provider up in a
    /// {@link ProcessorContext}. Convention: lowercase, one word
    /// (e.g. "config", "logging", "content").
    String name();

    /// Category — matches the type key in config.yaml when providers
    /// are defined there.
    String providerType();

    ComponentState state();

    void enable();

    void disable(int drainTimeoutSeconds);

    void shutdown();

    default boolean isEnabled() {
        return state() == ComponentState.ENABLED;
    }
}
