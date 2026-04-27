package zincflow.core;

import java.util.List;
import java.util.Map;

/// SPI for third-party sources. Drop a JAR into the plugin directory
/// that exposes a {@code SourcePlugin} service and the source becomes
/// usable from {@code config.yaml} under the {@code sources:} block,
/// the same way processors do under {@code flow.processors:}.
///
/// Convention: {@link #create(String, Map)} does not start the source;
/// lifecycle is {@link zincflow.fabric.Pipeline#startSource}'s job.
public interface SourcePlugin {

    /// Stable identifier used in {@code config.yaml}. Must be unique
    /// across all registered (type, version) pairs.
    String sourceType();

    /// Semver version for this source. Exposed via
    /// {@code GET /api/source-types} so config authors can pin via
    /// {@code type: MySource@1.2.0}. Default {@code "1.0.0"}.
    default String version() { return "1.0.0"; }

    /// Short description shown in the UI.
    default String description() { return ""; }

    /// Config keys the source accepts — surfaced to the UI so it can
    /// render an "add source" form.
    default List<String> configKeys() { return List.of(); }

    /// Instantiate the source given its config-file name and config
    /// map. {@code name} is the key under {@code sources:} in YAML —
    /// the source uses it as its own {@link Source#name()}.
    Source create(String name, Map<String, Object> config);
}
