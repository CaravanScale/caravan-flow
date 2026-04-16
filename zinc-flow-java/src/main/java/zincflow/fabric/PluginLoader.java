package zincflow.fabric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ProcessorContext;
import zincflow.core.ProcessorPlugin;
import zincflow.core.Provider;
import zincflow.core.ProviderPlugin;

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

    public record Summary(
            List<String> processorTypes,
            List<String> providerNames,
            Path directory,
            List<Path> jars) {
        public Summary {
            processorTypes = List.copyOf(processorTypes);
            providerNames = List.copyOf(providerNames);
            jars = List.copyOf(jars);
        }

        public static Summary empty() {
            return new Summary(List.of(), List.of(), null, List.of());
        }

        public int totalLoaded() {
            return processorTypes.size() + providerNames.size();
        }
    }

    /// Discover plugins on the given classloader and register them.
    /// Providers first, then processors — so processor plugins that
    /// require a provider get it via the context at create-time.
    public static Summary load(ClassLoader cl, Registry registry, ProcessorContext context) {
        List<String> providers = loadProviders(cl, context);
        List<String> processors = loadProcessors(cl, registry);
        return new Summary(processors, providers, null, List.of());
    }

    /// Scan {@code dir} for {@code *.jar} files, stitch them into a
    /// {@link URLClassLoader} layered on top of the current classloader,
    /// and register every plugin service they expose. Missing or empty
    /// directories are not an error — they yield an empty summary.
    public static Summary loadFromDirectory(Path dir, Registry registry, ProcessorContext context) {
        if (dir == null || !Files.isDirectory(dir)) {
            return new Summary(List.of(), List.of(), dir, List.of());
        }
        List<Path> jars = new ArrayList<>();
        try (Stream<Path> entries = Files.list(dir)) {
            entries.filter(p -> p.getFileName().toString().endsWith(".jar"))
                   .sorted()
                   .forEach(jars::add);
        } catch (IOException ex) {
            log.warn("plugin directory scan failed: {} — {}", dir, ex.toString());
            return new Summary(List.of(), List.of(), dir, List.of());
        }
        if (jars.isEmpty()) {
            return new Summary(List.of(), List.of(), dir, List.of());
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
        List<String> providers = loadProviders(cl, context);
        List<String> processors = loadProcessors(cl, registry);
        log.info("loaded {} plugin(s) from {} — providers: {}, processors: {}",
                providers.size() + processors.size(), dir, providers, processors);
        return new Summary(processors, providers, dir, jars);
    }

    private static List<String> loadProviders(ClassLoader cl, ProcessorContext context) {
        List<String> names = new ArrayList<>();
        for (ProviderPlugin plugin : ServiceLoader.load(ProviderPlugin.class, cl)) {
            try {
                Provider p = plugin.create();
                if (p == null) {
                    log.warn("ProviderPlugin {} returned null from create()", plugin.getClass().getName());
                    continue;
                }
                context.addProvider(p);
                names.add(p.name());
                log.info("plugin provider registered: {} ({})", p.name(), plugin.getClass().getName());
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
                    ? Registry.DEFAULT_VERSION : plugin.version();
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
        out.put("totalLoaded", s.totalLoaded());
        return out;
    }
}
