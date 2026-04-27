package zincflow.core;

import java.util.List;
import java.util.Map;

/// SPI for providers. Plugins surface shared infrastructure (metrics
/// sinks, schema registries, external caches, version control) through
/// the same {@link ProcessorContext} that built-in providers live in,
/// so processor plugins can require them the same way built-in
/// processors do.
///
/// Every built-in provider ships a {@code ProviderPlugin} through
/// {@code META-INF/services/zincflow.core.ProviderPlugin} — that's the
/// single discovery path for the full provider set. A plugin jar that
/// registers a higher-versioned entry under the same
/// {@link #providerType()} replaces the built-in at latest-version
/// lookup.
///
/// Convention: {@link #create(Map)} returns the provider already
/// constructed but not yet enabled; the boot path calls
/// {@link Provider#enable()} after inserting into the context.
/// Returning {@code null} means "disabled for this config" — e.g.
/// {@code UIRegistrationProvider} factories return null when
/// {@code ui.register_to} is absent.
public interface ProviderPlugin {

    /// Stable identifier — maps to the {@code type:} field under a
    /// {@code providers:} block in config.yaml. Must be unique across
    /// registered (type, version) pairs.
    String providerType();

    /// Semver version. Default {@code "1.0.0"}. A plugin jar with a
    /// higher version than the built-in of the same type replaces the
    /// built-in at unqualified lookup.
    default String version() { return "1.0.0"; }

    /// Short description shown in the UI.
    default String description() { return ""; }

    /// Config keys the provider accepts — surfaced to admin tooling.
    default List<String> configKeys() { return List.of(); }

    /// Instantiate the provider from its config. Return {@code null}
    /// when the provider should be skipped (e.g. a conditional provider
    /// whose enabling config key is absent). Return a non-null
    /// {@link Provider} to have it added to the context and enabled.
    Provider create(Map<String, Object> config);
}
