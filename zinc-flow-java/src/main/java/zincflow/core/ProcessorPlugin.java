package zincflow.core;

import java.util.Map;

/// SPI for third-party processors. Drop a JAR into the plugins directory
/// with a {@code META-INF/services/zincflow.core.ProcessorPlugin} entry
/// listing fully qualified class names, and the zinc-flow-java
/// {@link zincflow.fabric.PluginLoader} registers each one under its
/// {@link #type()} with the {@link zincflow.fabric.Registry}.
///
/// Implementations must have a public no-arg constructor —
/// {@link java.util.ServiceLoader} requires it. The {@link #create}
/// method does all the real work, with access to the pipeline's
/// {@link ProcessorContext} so plugins can pull shared infrastructure
/// (content store, logger, ...) the same way built-in processors do.
public interface ProcessorPlugin {

    /// Stable identifier used in {@code config.yaml} and
    /// {@code /api/processors/add}. Must be unique across all registered
    /// processors — a second plugin with the same type overwrites the
    /// first (last loader wins).
    String type();

    Processor create(Map<String, String> config, ProcessorContext context);
}
