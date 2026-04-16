package zincflow.core;

/// SPI for third-party providers. Plugins surface shared infrastructure
/// (metrics sinks, schema registries, external caches) through the
/// same {@link ProcessorContext} that built-in providers live in, so
/// processor plugins can require them the same way built-in processors
/// do.
///
/// Convention: {@link #create()} returns the provider in whichever
/// lifecycle state the plugin wants operators to see at startup.
/// Typically that's ENABLED — disabling is an operator action via
/// {@code POST /api/providers/disable}.
public interface ProviderPlugin {

    /// Instantiate the provider. Called once at startup; the returned
    /// instance is added to the {@link ProcessorContext} by
    /// {@link zincflow.fabric.PluginLoader}.
    Provider create();
}
