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
    /// (type, version) pairs — a second plugin with the same type + version
    /// overwrites the first (last loader wins).
    String type();

    /// Semver version for this processor. Exposed via
    /// {@code GET /api/processor-types} so config authors can pin via
    /// {@code type: MyProc@1.2.0}. Default {@code "1.0.0"} keeps
    /// pre-versioning plugins compiling without changes.
    default String version() { return "1.0.0"; }

    /// Short description shown in the UI when browsing processor types.
    default String description() { return ""; }

    /// Config keys the processor accepts — surfaced to the UI so it
    /// can render an "add processor" form.
    default java.util.List<String> configKeys() { return java.util.List.of(); }

    /// Result relationships this processor may produce
    /// (e.g. {@link Relationships#SUCCESS}, {@link Relationships#FAILURE},
    /// {@link Relationships#MATCHED}). Used by the UI connection editor
    /// to show the outbound ports.
    default java.util.List<String> relationships() {
        return java.util.List.of(Relationships.SUCCESS, Relationships.FAILURE);
    }

    Processor create(Map<String, String> config, ProcessorContext context);
}
