package caravanflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import caravanflow.core.Processor;
import caravanflow.core.ProcessorContext;
import caravanflow.core.Provider;
import caravanflow.core.Relationships;
import caravanflow.fabric.ConfigLoader;
import caravanflow.fabric.HttpServer;
import caravanflow.fabric.Metrics;
import caravanflow.fabric.NodeIdentity;
import caravanflow.fabric.Pipeline;
import caravanflow.fabric.PipelineGraph;
import caravanflow.fabric.PluginLoader;
import caravanflow.fabric.ProviderRegistry;
import caravanflow.fabric.Registry;
import caravanflow.fabric.SourceRegistry;
import caravanflow.fabric.TypeRefs;
import caravanflow.processors.LogAttribute;
import caravanflow.processors.RouteOnAttribute;
import caravanflow.processors.UpdateAttribute;
import caravanflow.providers.UIRegistrationProvider;
import caravanflow.providers.VersionControlProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/// Entry point. Loads a config.yaml if one is present (arg 1 or
/// ./config.yaml), otherwise falls back to a built-in demo pipeline so
/// `caravan run` with no config still produces something useful.
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // --- Config key paths (relative to the effective layered map) -------
    private static final String CFG_UI           = "ui";
    private static final String CFG_UI_REGISTER  = "register_to";
    private static final String CFG_VC           = "vc";
    private static final String CFG_VC_ENABLED   = "enabled";
    private static final String CFG_VC_REPO      = "repo";
    private static final String CFG_VC_GIT       = "git";
    private static final String CFG_VC_REMOTE    = "remote";
    private static final String CFG_VC_BRANCH    = "branch";

    // --- Runtime env / system properties --------------------------------
    private static final String ENV_PLUGINS_DIR  = "CARAVANFLOW_PLUGINS_DIR";
    private static final String PROP_PLUGINS_DIR = "caravanflow.pluginsDir";
    private static final String PROP_PORT        = "caravanflow.port";
    private static final String DEFAULT_PORT     = "9092";

    public static void main(String[] args) throws IOException {
        Path configPath = resolveConfigPath(args);
        Registry registry = new Registry();
        SourceRegistry sourceRegistry = new SourceRegistry();
        ProviderRegistry providerRegistry = new ProviderRegistry();
        ProcessorContext context = new ProcessorContext();

        // Populate registries via ServiceLoader — same path plugin jars
        // use, so built-ins and plugins are discovered uniformly.
        PluginLoader.loadSources(Main.class.getClassLoader(), sourceRegistry);
        PluginLoader.loadProviders(Main.class.getClassLoader(), providerRegistry);

        // Register the two bootstrap-dependent providers (identity
        // supplier, resolved repo path) as registry factories that
        // close over Main-local state. An AtomicReference lets the
        // UIReg factory run before identity is resolved — the supplier
        // is called lazily at heartbeat time, not construct time.
        var identityRef = new java.util.concurrent.atomic.AtomicReference<NodeIdentity>();
        registerBootstrapProviders(providerRegistry, identityRef, configPath);

        // Plugin discovery — scan $CARAVANFLOW_PLUGINS_DIR (default ./plugins)
        // for third-party processor/provider jars before loading the flow,
        // so config.yaml can reference plugin-provided types.
        Path pluginsDir = resolvePluginsDir();
        PluginLoader.Summary plugins = PluginLoader.loadFromDirectory(
                pluginsDir, registry, context, sourceRegistry);
        if (plugins.totalLoaded() > 0) {
            log.info("loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
                    plugins.totalLoaded(), pluginsDir, plugins.providerNames(),
                    plugins.processorTypes(), plugins.sourceTypes());
        }

        ConfigLoader loader = new ConfigLoader(registry, context, sourceRegistry, providerRegistry);
        PipelineGraph graph;
        if (configPath != null && Files.isRegularFile(configPath)) {
            log.info("loading pipeline from {}", configPath.toAbsolutePath());
            graph = loader.loadFromFile(configPath);
        } else {
            log.info("no config.yaml found — using built-in demo pipeline");
            graph = demoGraph();
        }
        Metrics metrics = new Metrics();
        Pipeline pipeline = new Pipeline(graph, Pipeline.DEFAULT_MAX_HOPS, metrics, context, registry);

        // Resolve node identity from the effective layered config (or
        // the persisted UUID file), then populate the atomic so the
        // UIReg factory's lazy identity supplier can see it.
        Map<String, Object> effective = loader.lastOverlay() == null ? Map.of()
                : loader.lastOverlay().effective();
        NodeIdentity identity = NodeIdentity.resolve(effective,
                Path.of(NodeIdentity.NODE_ID_FILE), Main.class.getPackage().getImplementationVersion());
        identityRef.set(identity);

        // Wire providers. The providers: block in config — if present —
        // takes precedence. Otherwise we instantiate every registered
        // type with its config map drawn from the effective overlay,
        // which preserves the default set (logging, config, provenance,
        // content, schema_registry) plus conditional ones (UIReg when
        // ui.register_to is set, VC when vc.enabled is true).
        List<Provider> providersToWire = loader.lastProviders().isEmpty()
                ? defaultProviders(providerRegistry, effective)
                : loader.lastProviders();
        for (Provider p : providersToWire) {
            context.addProvider(p);
            p.enable();
            log.info("provider {} ({}) enabled", p.name(), p.providerType());
        }

        // Register + auto-start every source configured under
        // sources:. Sources emit FlowFiles into pipeline.ingest() the
        // same way HTTP POST / does, so the graph's entryPoints see
        // them indistinguishably.
        for (var source : loader.lastSources()) {
            pipeline.addSource(source);
            pipeline.startSource(source.name());
            log.info("source {} ({}) started", source.name(), source.sourceType());
        }

        int port = resolvePort();
        HttpServer server = new HttpServer(pipeline, loader, configPath, plugins, pluginsDir, identity).start(port);
        log.info("caravan-flow-java up — node {} at http://localhost:{} (hostname {})",
                identity.nodeId(), server.port(), identity.hostname());
        log.info("dashboard: GET http://localhost:{}/dashboard    metrics: /metrics", server.port());

        registerShutdownHook(server, pipeline, context);
    }

    /// Stop sources, drop the HTTP socket, and quiesce providers on
    /// SIGTERM. Runs on a dedicated platform thread — {@code shutdown()}
    /// implementations must be fast and non-blocking (they already are).
    private static void registerShutdownHook(HttpServer server, Pipeline pipeline, ProcessorContext context) {
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("caravan-flow-shutdown").unstarted(() -> {
            log.info("shutdown signal received — stopping worker");
            for (var source : pipeline.sources().values()) {
                try { source.stop(); }
                catch (RuntimeException ex) { log.warn("source {} stop failed: {}", source.name(), ex.toString()); }
            }
            try { server.stop(); }
            catch (RuntimeException ex) { log.warn("server stop failed: {}", ex.toString()); }
            try { context.shutdownAll(); }
            catch (RuntimeException ex) { log.warn("provider shutdown failed: {}", ex.toString()); }
            log.info("worker stopped cleanly");
        }));
    }

    private static int resolvePort() {
        return Integer.parseInt(System.getProperty(PROP_PORT, DEFAULT_PORT));
    }

    /// Register the two providers whose construction depends on
    /// Main-local state (node identity, config file location) as
    /// closures over that state. Done before config load so the
    /// {@code providers:} block can reference them.
    private static void registerBootstrapProviders(
            ProviderRegistry providerRegistry,
            java.util.concurrent.atomic.AtomicReference<NodeIdentity> identityRef,
            Path configPath) {
        providerRegistry.register(
                new ProviderRegistry.TypeInfo(
                        UIRegistrationProvider.TYPE, TypeRefs.DEFAULT_VERSION,
                        "Self-registers this worker with a central UI via periodic heartbeat.",
                        List.of(CFG_UI_REGISTER)),
                cfg -> {
                    Object target = cfg.get(CFG_UI_REGISTER);
                    if (target == null || target.toString().isEmpty()) return null;
                    return new UIRegistrationProvider(target.toString(),
                            () -> identityRef.get().toMap(resolvePort()));
                });

        providerRegistry.register(
                new ProviderRegistry.TypeInfo(
                        VersionControlProvider.TYPE, TypeRefs.DEFAULT_VERSION,
                        "Shells out to system git for flow-config commit + push.",
                        List.of(CFG_VC_ENABLED, CFG_VC_REPO, CFG_VC_GIT, CFG_VC_REMOTE, CFG_VC_BRANCH)),
                cfg -> {
                    Object enabled = cfg.get(CFG_VC_ENABLED);
                    boolean on = enabled instanceof Boolean b ? b
                            : "true".equalsIgnoreCase(String.valueOf(enabled));
                    if (!on) return null;
                    Path repo = cfg.containsKey(CFG_VC_REPO)
                            ? Path.of(String.valueOf(cfg.get(CFG_VC_REPO)))
                            : (configPath == null ? Path.of(".") : configPath.toAbsolutePath().getParent());
                    return new VersionControlProvider(
                            repo,
                            String.valueOf(cfg.getOrDefault(CFG_VC_GIT,    "git")),
                            String.valueOf(cfg.getOrDefault(CFG_VC_REMOTE, "origin")),
                            String.valueOf(cfg.getOrDefault(CFG_VC_BRANCH, "main")));
                });
    }

    /// Build the default provider set when {@code providers:} is absent
    /// from config. For every registered provider type we call its
    /// factory with whatever matches the conventional config key
    /// (e.g. the {@code ui} / {@code vc} sub-maps for UIReg / VC) or
    /// an empty map for the stateless built-ins. Null factory returns
    /// mean "disabled" and are filtered out.
    @SuppressWarnings("unchecked")
    private static List<Provider> defaultProviders(
            ProviderRegistry providerRegistry, Map<String, Object> effective) {
        List<Provider> out = new java.util.ArrayList<>();
        for (ProviderRegistry.TypeInfo info : providerRegistry.listAll()) {
            Map<String, Object> cfg = switch (info.name()) {
                case UIRegistrationProvider.TYPE -> effective.get(CFG_UI) instanceof Map<?, ?> m
                        ? (Map<String, Object>) m : Map.of();
                case VersionControlProvider.TYPE -> effective.get(CFG_VC) instanceof Map<?, ?> m
                        ? (Map<String, Object>) m : Map.of();
                default -> Map.of();
            };
            Provider p = providerRegistry.create(info.qualifiedName(), cfg);
            if (p != null) out.add(p);
        }
        return out;
    }

    private static Path resolveConfigPath(String[] args) {
        if (args.length > 0) return Path.of(args[0]);
        Path defaultPath = Path.of("config.yaml");
        return Files.isRegularFile(defaultPath) ? defaultPath : null;
    }

    /// Pick the plugins directory — {@code $CARAVANFLOW_PLUGINS_DIR} when
    /// set, otherwise {@code ./plugins}. Non-existent paths are fine;
    /// {@link PluginLoader#loadFromDirectory} returns an empty summary
    /// in that case so the CLI path doesn't hard-fail on a fresh
    /// checkout that hasn't shipped plugins yet.
    private static Path resolvePluginsDir() {
        String envDir = System.getenv(ENV_PLUGINS_DIR);
        if (envDir != null && !envDir.isEmpty()) return Path.of(envDir);
        String propDir = System.getProperty(PROP_PLUGINS_DIR);
        if (propDir != null && !propDir.isEmpty()) return Path.of(propDir);
        return Path.of("plugins");
    }

    /// Built-in demo pipeline used when no config.yaml is provided.
    /// Mirrors the Phase 2 hard-coded graph.
    static PipelineGraph demoGraph() {
        Processor ingress = new LogAttribute("[ingress] ");
        Processor router = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        Processor elevate = new UpdateAttribute("priority", "elevated");
        Processor tail = new LogAttribute("[tail] ");

        Map<String, Processor> processors = Map.of(
                "ingress", ingress,
                "router",  router,
                "elevate", elevate,
                "tail",    tail);
        Map<String, Map<String, List<String>>> connections = Map.of(
                "ingress", Map.of(Relationships.SUCCESS,   List.of("router")),
                "router",  Map.of(
                        "high",                   List.of("elevate"),
                        "low",                    List.of("tail"),
                        Relationships.UNMATCHED,  List.of("tail")),
                "elevate", Map.of(Relationships.SUCCESS,   List.of("tail")));
        return new PipelineGraph(processors, connections, List.of("ingress"));
    }
}
