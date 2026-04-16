package zincflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.Processor;
import zincflow.fabric.ConfigLoader;
import zincflow.fabric.HttpServer;
import zincflow.fabric.Pipeline;
import zincflow.fabric.PipelineGraph;
import zincflow.fabric.Registry;
import zincflow.processors.LogAttribute;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.UpdateAttribute;

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
        ConfigLoader loader = new ConfigLoader(registry);
        PipelineGraph graph;
        if (configPath != null && Files.isRegularFile(configPath)) {
            log.info("loading pipeline from {}", configPath.toAbsolutePath());
            graph = loader.loadFromFile(configPath);
        } else {
            log.info("no config.yaml found — using built-in demo pipeline");
            graph = demoGraph();
        }
        Pipeline pipeline = new Pipeline(graph);
        int port = Integer.parseInt(System.getProperty("zincflow.port", "9092"));
        HttpServer server = new HttpServer(pipeline, loader, configPath).start(port);
        log.info("zinc-flow-java up — POST to http://localhost:{}/ to ingest", server.port());
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
