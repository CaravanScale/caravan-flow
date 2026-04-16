package zincflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.MemoryContentStore;
import zincflow.core.Processor;
import zincflow.core.ProcessorContext;
import zincflow.fabric.ConfigLoader;
import zincflow.fabric.HttpServer;
import zincflow.fabric.Metrics;
import zincflow.fabric.Pipeline;
import zincflow.fabric.PipelineGraph;
import zincflow.fabric.Registry;
import zincflow.processors.LogAttribute;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.UpdateAttribute;
import zincflow.providers.ConfigProvider;
import zincflow.providers.ContentProvider;
import zincflow.providers.LoggingProvider;
import zincflow.providers.ProvenanceProvider;

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

        ConfigLoader loader = new ConfigLoader(registry, context);
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
        int port = Integer.parseInt(System.getProperty("zincflow.port", "9092"));
        HttpServer server = new HttpServer(pipeline, loader, configPath).start(port);
        log.info("zinc-flow-java up — POST to http://localhost:{}/ to ingest", server.port());
        log.info("dashboard: GET http://localhost:{}/dashboard    metrics: /metrics", server.port());
    }

    private static Path resolveConfigPath(String[] args) {
        if (args.length > 0) return Path.of(args[0]);
        Path defaultPath = Path.of("config.yaml");
        return Files.isRegularFile(defaultPath) ? defaultPath : null;
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
