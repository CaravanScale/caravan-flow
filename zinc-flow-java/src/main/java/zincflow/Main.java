package zincflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.MemoryContentStore;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.fabric.ConfigLoader;
import zincflow.fabric.HttpServer;
import zincflow.fabric.Metrics;
import zincflow.fabric.NodeIdentity;
import zincflow.fabric.Pipeline;
import zincflow.fabric.PipelineGraph;
import zincflow.fabric.PluginLoader;
import zincflow.fabric.Registry;
import zincflow.fabric.SourceRegistry;
import zincflow.processors.LogAttribute;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.UpdateAttribute;
import zincflow.providers.ConfigProvider;
import zincflow.providers.ContentProvider;
import zincflow.providers.LoggingProvider;
import zincflow.providers.ProvenanceProvider;
import zincflow.providers.SchemaRegistryProvider;
import zincflow.providers.UIRegistrationProvider;
import zincflow.providers.VersionControlProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/// Entry point. Loads a config.yaml if one is present (arg 1 or
/// ./config.yaml), otherwise falls back to a built-in demo pipeline so
/// `zinc run` with no config still produces something useful.
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        Path configPath = resolveConfigPath(args);
        Registry registry = new Registry();
        SourceRegistry sourceRegistry = new SourceRegistry();
        // Built-in sources register via the system classloader's
        // META-INF/services/zincflow.core.SourcePlugin so they come in
        // through the same ServiceLoader path that plugin jars use.
        // A plugin jar with the same type name + higher version wins
        // at latest-version lookup.
        PluginLoader.loadSources(Main.class.getClassLoader(), sourceRegistry);

        // Wire the Phase 3e provider set — config, logging, provenance,
        // and an in-memory content store. All start ENABLED so the
        // pipeline is useful out of the box; operators disable individual
        // providers at runtime via POST /api/providers/disable.
        ProcessorContext context = new ProcessorContext();
        LoggingProvider logging = new LoggingProvider();
        logging.enable();
        context.addProvider(logging);
        ConfigProvider cfg = new ConfigProvider(Map.of());
        cfg.enable();
        context.addProvider(cfg);
        ProvenanceProvider provenance = new ProvenanceProvider();
        provenance.enable();
        context.addProvider(provenance);
        ContentProvider content = new ContentProvider(new MemoryContentStore());
        content.enable();
        context.addProvider(content);
        SchemaRegistryProvider schemaRegistry = new SchemaRegistryProvider();
        schemaRegistry.enable();
        context.addProvider(schemaRegistry);

        // Plugin discovery — scan $ZINCFLOW_PLUGINS_DIR (default ./plugins)
        // for third-party processor/provider jars before loading the flow,
        // so config.yaml can reference plugin-provided types.
        Path pluginsDir = resolvePluginsDir();
        PluginLoader.Summary plugins = PluginLoader.loadFromDirectory(pluginsDir, registry, context, sourceRegistry);
        if (plugins.totalLoaded() > 0) {
            log.info("loaded {} plugin(s) from {} — providers: {}, processors: {}, sources: {}",
                    plugins.totalLoaded(), pluginsDir, plugins.providerNames(),
                    plugins.processorTypes(), plugins.sourceTypes());
        }

        ConfigLoader loader = new ConfigLoader(registry, context, sourceRegistry);
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

        // Node identity — read ui.nodeId from the effective layered
        // config if set, else use/create the persisted UUID file.
        Map<String, Object> effective = loader.lastOverlay() == null ? Map.of()
                : loader.lastOverlay().effective();
        NodeIdentity identity = NodeIdentity.resolve(effective,
                Path.of(NodeIdentity.NODE_ID_FILE), Main.class.getPackage().getImplementationVersion());

        // Optional UI self-registration — only if ui.register_to is set.
        String uiTarget = registerTarget(effective);
        if (uiTarget != null) {
            UIRegistrationProvider reg = new UIRegistrationProvider(uiTarget,
                    () -> identity.toMap(Integer.parseInt(System.getProperty("zincflow.port", "9092"))));
            context.addProvider(reg);
            reg.enable();
            log.info("self-registering with UI at {}", uiTarget);
        }

        // Optional Git version-control provider — only if vc.enabled: true.
        if (isVcEnabled(effective)) {
            VersionControlProvider vc = buildVcProvider(effective, configPath);
            context.addProvider(vc);
            vc.enable();
            log.info("version control enabled — repo={} branch={} remote={}",
                    vc.repo(), vc.branch(), vc.remote());
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

        int port = Integer.parseInt(System.getProperty("zincflow.port", "9092"));
        HttpServer server = new HttpServer(pipeline, loader, configPath, plugins, pluginsDir, identity).start(port);
        log.info("zinc-flow-java up — node {} at http://localhost:{} (hostname {})",
                identity.nodeId(), server.port(), identity.hostname());
        log.info("dashboard: GET http://localhost:{}/dashboard    metrics: /metrics", server.port());
    }

    @SuppressWarnings("unchecked")
    private static String registerTarget(Map<String, Object> effective) {
        Object ui = effective.get("ui");
        if (!(ui instanceof Map<?, ?> uiMap)) return null;
        Object target = ((Map<String, Object>) uiMap).get("register_to");
        return target == null ? null : target.toString();
    }

    @SuppressWarnings("unchecked")
    private static boolean isVcEnabled(Map<String, Object> effective) {
        Object vc = effective.get("vc");
        if (!(vc instanceof Map<?, ?> vcMap)) return false;
        Object enabled = ((Map<String, Object>) vcMap).get("enabled");
        return enabled instanceof Boolean b ? b : "true".equalsIgnoreCase(String.valueOf(enabled));
    }

    @SuppressWarnings("unchecked")
    private static VersionControlProvider buildVcProvider(Map<String, Object> effective, Path configPath) {
        Object vc = effective.get("vc");
        Map<String, Object> vcMap = vc instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        Path repo = vcMap.containsKey("repo")
                ? Path.of(String.valueOf(vcMap.get("repo")))
                : (configPath == null ? Path.of(".") : configPath.toAbsolutePath().getParent());
        String gitBinary = vcMap.containsKey("git") ? String.valueOf(vcMap.get("git")) : "git";
        String remote = vcMap.containsKey("remote") ? String.valueOf(vcMap.get("remote")) : "origin";
        String branch = vcMap.containsKey("branch") ? String.valueOf(vcMap.get("branch")) : "main";
        return new VersionControlProvider(repo, gitBinary, remote, branch);
    }

    private static Path resolveConfigPath(String[] args) {
        if (args.length > 0) return Path.of(args[0]);
        Path defaultPath = Path.of("config.yaml");
        return Files.isRegularFile(defaultPath) ? defaultPath : null;
    }

    /// Pick the plugins directory — {@code $ZINCFLOW_PLUGINS_DIR} when
    /// set, otherwise {@code ./plugins}. Non-existent paths are fine;
    /// {@link PluginLoader#loadFromDirectory} returns an empty summary
    /// in that case so the CLI path doesn't hard-fail on a fresh
    /// checkout that hasn't shipped plugins yet.
    private static Path resolvePluginsDir() {
        String envDir = System.getenv("ZINCFLOW_PLUGINS_DIR");
        if (envDir != null && !envDir.isEmpty()) return Path.of(envDir);
        String propDir = System.getProperty("zincflow.pluginsDir");
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
                "ingress", Map.of("success",   List.of("router")),
                "router",  Map.of(
                        "high",      List.of("elevate"),
                        "low",       List.of("tail"),
                        "unmatched", List.of("tail")),
                "elevate", Map.of("success",   List.of("tail")));
        return new PipelineGraph(processors, connections, List.of("ingress"));
    }
}
