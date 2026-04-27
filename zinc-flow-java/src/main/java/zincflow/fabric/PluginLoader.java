package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorPlugin;
import zincflow.core.Provider;
import zincflow.core.ProviderPlugin;
import zincflow.core.SourcePlugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/// Discovers {@link ProcessorPlugin} and {@link ProviderPlugin}
/// services on a given {@link ClassLoader} and wires them into a
/// {@link Registry} + {@link ProcessorContext}. Providers load first
/// so processor plugins built on top of them find the dependency in
/// the context.
///
/// <h2>Shape</h2>
/// Two static entry points:
/// <ul>
///   <li>{@link #load(ClassLoader, Registry, ProcessorContext)} — scan
///       a specific classloader. Used in tests + for the JVM's system
///       classloader after an ad-hoc add.</li>
///   <li>{@link #loadFromDirectory(Path, Registry, ProcessorContext)} —
///       build a {@link URLClassLoader} over every {@code *.jar} in
///       the directory, then scan it. Used in production via the
///       {@code $ZINCFLOW_PLUGINS_DIR} (default {@code ./plugins}) hook
///       in {@link zincflow.Main}.</li>
/// </ul>
///
/// Both return a {@link Summary} listing the registered names so the
/// management API and startup logs can report what came in.
public final class PluginLoader {

    private static final Logger log = LoggerFactory.getLogger(PluginLoader.class);

    private PluginLoader() {}

    /// {@code classLoader} is the {@link URLClassLoader} spawned for the
    /// plugin jars — retained on the summary so the owner (typically
    /// {@link HttpServer}) can close it before replacing it on the next
    /// {@code POST /api/plugins/reload}. Null when no classloader was
    /// created (empty directory, classpath-only scan).
    public record Summary(
            List<String> processorTypes,
            List<String> providerNames,
            List<String> sourceTypes,
            Path directory,
            List<Path> jars,
            java.net.URLClassLoader classLoader) implements AutoCloseable {
        public Summary {
            processorTypes = List.copyOf(processorTypes);
            providerNames = List.copyOf(providerNames);
            sourceTypes = List.copyOf(sourceTypes);
            jars = List.copyOf(jars);
        }

        public static Summary empty() {
            return new Summary(List.of(), List.of(), List.of(), null, List.of(), null);
        }

        public int totalLoaded() {
            return processorTypes.size() + providerNames.size() + sourceTypes.size();
        }

        /// Release the {@link URLClassLoader} — best effort. Safe to call
        /// on an {@link #empty()} summary.
        @Override
        public void close() {
            if (classLoader == null) return;
            try { classLoader.close(); }
            catch (IOException ex) {
                log.warn("failed to close plugin classloader: {}", ex.toString());
            }
        }
    }

    /// Discover plugins on the given classloader and register them.
    /// Providers first (processors may require them at create-time),
    /// then processors, then sources (sources are independent but their
    /// factories may look up providers via the context at start-time).
    public static Summary load(ClassLoader cl, Registry registry, ProcessorContext context) {
        return load(cl, registry, context, null);
    }

    public static Summary load(ClassLoader cl, Registry registry, ProcessorContext context,
                               SourceRegistry sourceRegistry) {
        List<String> providers = loadProvidersLegacy(cl, context);
        List<String> processors = loadProcessors(cl, registry);
        List<String> sources = sourceRegistry == null ? List.of() : loadSources(cl, sourceRegistry);
        return new Summary(processors, providers, sources, null, List.of(), null);
    }

    /// Scan {@code dir} for {@code *.jar} files, stitch them into a
    /// {@link URLClassLoader} layered on top of the current classloader,
    /// and register every plugin service they expose. Missing or empty
    /// directories are not an error — they yield an empty summary.
    public static Summary loadFromDirectory(Path dir, Registry registry, ProcessorContext context) {
        return loadFromDirectory(dir, registry, context, null);
    }

    public static Summary loadFromDirectory(Path dir, Registry registry, ProcessorContext context,
                                            SourceRegistry sourceRegistry) {
        if (dir == null || !Files.isDirectory(dir)) {
            return new Summary(List.of(), List.of(), List.of(), dir, List.of(), null);
        }
        List<Path> jars = new ArrayList<>();
        try (Stream<Path> entries = Files.list(dir)) {
            entries.filter(p -> p.getFileName().toString().endsWith(".jar"))
                   .sorted()
                   .forEach(jars::add);
        } catch (IOException ex) {
            log.warn("plugin directory scan failed: {} — {}", dir, ex.toString());
            return new Summary(List.of(), List.of(), List.of(), dir, List.of(), null);
        }
        if (jars.isEmpty()) {
            return new Summary(List.of(), List.of(), List.of(), dir, List.of(), null);
        }
        URL[] urls = new URL[jars.size()];
        for (int i = 0; i < jars.size(); i++) {
            try {
                urls[i] = jars.get(i).toUri().toURL();
            } catch (MalformedURLException ex) {
                // toUri().toURL() on an absolute path doesn't realistically fail,
                // but if it does the plugin is unusable — skip with a warning.
                log.warn("plugin jar has unusable URL: {} — {}", jars.get(i), ex.toString());
                urls[i] = null;
            }
        }
        URLClassLoader cl = new URLClassLoader(stripNulls(urls), PluginLoader.class.getClassLoader());
        List<String> providers = loadProvidersLegacy(cl, context);
        List<String> processors = loadProcessors(cl, registry);
        List<String> sources = sourceRegistry == null ? List.of() : loadSources(cl, sourceRegistry);
        log.info("loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
                providers.size() + processors.size() + sources.size(), dir, providers, processors, sources);
        return new Summary(processors, providers, sources, dir, jars, cl);
    }

    /// Scan {@code cl} for {@link ProviderPlugin} services and register
    /// their factories with {@code registry}. Actual provider
    /// instantiation is deferred to config loading — the registry maps
    /// {@code type: LoggingProvider@1.0.0} to a factory the same way
    /// processors and sources do.
    public static List<String> loadProviders(ClassLoader cl, ProviderRegistry registry) {
        List<String> types = new ArrayList<>();
        for (ProviderPlugin plugin : ServiceLoader.load(ProviderPlugin.class, cl)) {
            String type = plugin.providerType();
            if (type == null || type.isEmpty()) {
                log.warn("ProviderPlugin {} reported blank providerType() — skipping", plugin.getClass().getName());
                continue;
            }
            String version = plugin.version() == null || plugin.version().isEmpty()
                    ? TypeRefs.DEFAULT_VERSION : plugin.version();
            ProviderRegistry.TypeInfo info = new ProviderRegistry.TypeInfo(
                    type, version,
                    plugin.description() == null ? "" : plugin.description(),
                    plugin.configKeys() == null ? List.of() : plugin.configKeys());
            registry.register(info, plugin::create);
            types.add(type + "@" + version);
            log.info("plugin provider registered: {}@{} ({})",
                    type, version, plugin.getClass().getName());
        }
        Collections.sort(types);
        return types;
    }

    /// Legacy path — drop a plugin directly into a {@link ProcessorContext}
    /// without going through a registry. Used when config.yaml has no
    /// {@code providers:} block and the caller wants everything pulled
    /// in with default config.
    private static List<String> loadProvidersLegacy(ClassLoader cl, ProcessorContext context) {
        List<String> names = new ArrayList<>();
        for (ProviderPlugin plugin : ServiceLoader.load(ProviderPlugin.class, cl)) {
            try {
                Provider p = plugin.create(Map.of());
                if (p == null) continue;
                // Skip if the bootstrap already wired a provider under
                // this name — Main instantiates a default set before
                // scanning plugins, and we don't want a built-in
                // ServiceLoader entry (from the main jar itself) to
                // overwrite the instance that's already enabled.
                if (context.getProvider(p.name()) != null) continue;
                context.addProvider(p);
                p.enable();
                names.add(p.name());
            } catch (RuntimeException ex) {
                log.warn("ProviderPlugin {} threw on create: {}", plugin.getClass().getName(), ex.toString());
            }
        }
        Collections.sort(names);
        return names;
    }

    private static List<String> loadProcessors(ClassLoader cl, Registry registry) {
        List<String> types = new ArrayList<>();
        for (ProcessorPlugin plugin : ServiceLoader.load(ProcessorPlugin.class, cl)) {
            String type = plugin.type();
            if (type == null || type.isEmpty()) {
                log.warn("ProcessorPlugin {} reported blank type() — skipping", plugin.getClass().getName());
                continue;
            }
            String version = plugin.version() == null || plugin.version().isEmpty()
                    ? TypeRefs.DEFAULT_VERSION : plugin.version();
            Registry.TypeInfo info = new Registry.TypeInfo(
                    type, version,
                    plugin.description() == null ? "" : plugin.description(),
                    plugin.configKeys() == null ? List.of() : plugin.configKeys(),
                    plugin.relationships() == null ? List.of() : plugin.relationships());
            registry.register(info, plugin::create);
            types.add(type + "@" + version);
            log.info("plugin processor registered: {}@{} ({})",
                    type, version, plugin.getClass().getName());
        }
        Collections.sort(types);
        return types;
    }

    /// Scan {@code cl} for {@link SourcePlugin} services and register
    /// them with {@code registry}. Exposed for the Main bootstrap so
    /// the built-in sources (shipped as ServiceLoader entries in the
    /// main jar) populate the registry through the same path as
    /// plugin-jar sources.
    public static List<String> loadSources(ClassLoader cl, SourceRegistry registry) {
        List<String> types = new ArrayList<>();
        for (SourcePlugin plugin : ServiceLoader.load(SourcePlugin.class, cl)) {
            String type = plugin.sourceType();
            if (type == null || type.isEmpty()) {
                log.warn("SourcePlugin {} reported blank sourceType() — skipping", plugin.getClass().getName());
                continue;
            }
            String version = plugin.version() == null || plugin.version().isEmpty()
                    ? TypeRefs.DEFAULT_VERSION : plugin.version();
            SourceRegistry.TypeInfo info = new SourceRegistry.TypeInfo(
                    type, version,
                    plugin.description() == null ? "" : plugin.description(),
                    plugin.configKeys() == null ? List.of() : plugin.configKeys());
            registry.register(info, plugin::create);
            types.add(type + "@" + version);
            log.info("plugin source registered: {}@{} ({})",
                    type, version, plugin.getClass().getName());
        }
        Collections.sort(types);
        return types;
    }

    private static URL[] stripNulls(URL[] in) {
        int keep = 0;
        for (URL u : in) if (u != null) keep++;
        URL[] out = new URL[keep];
        int j = 0;
        for (URL u : in) if (u != null) out[j++] = u;
        return out;
    }

    /// Convenience — expose a summary as a JSON-friendly map for the
    /// management API. Avoids Jackson having to reflect over the record.
    public static Map<String, Object> toJson(Summary s) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("directory", s.directory() == null ? null : s.directory().toString());
        out.put("jars", s.jars().stream().map(Path::toString).toList());
        out.put("processorTypes", s.processorTypes());
        out.put("providerNames", s.providerNames());
        out.put("sourceTypes", s.sourceTypes());
        out.put("totalLoaded", s.totalLoaded());
        return out;
    }
}
