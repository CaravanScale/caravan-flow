package caravanflow.core;

/// Shared lifecycle contract for providers, implemented by the built-in
/// ConfigProvider, LoggingProvider, ProvenanceProvider, and
/// ContentProvider. User code can implement to expose custom shared
/// infrastructure (metrics sinks, cache pools, schema registries).
/// Mirrors caravan-flow-csharp's IProvider.
public interface Provider {

    /// Runtime identifier used to look the provider up in a
    /// {@link ProcessorContext}. Convention: lowercase, one word
    /// (e.g. {@code "config"}, {@code "logging"}, {@code "content"}).
    /// Exposed as {@code NAME} on every built-in provider.
    String name();

    /// Registry-facing type identifier — the string a config file
    /// puts under {@code type:} and that {@link ProviderPlugin} uses
    /// to key its factory. Convention: CamelCase class-style
    /// (e.g. {@code "LoggingProvider"}, {@code "ContentProvider"}).
    /// Exposed as {@code TYPE} on every built-in provider.
    ///
    /// <p>Two identifiers is intentional: {@code name()} is the
    /// instance-local key ("which provider in this context"), while
    /// {@code providerType()} is the factory-level key ("what kind
    /// of provider is this").
    String providerType();

    ComponentState state();

    void enable();

    void disable(int drainTimeoutSeconds);

    void shutdown();

    default boolean isEnabled() {
        return state() == ComponentState.ENABLED;
    }
}
