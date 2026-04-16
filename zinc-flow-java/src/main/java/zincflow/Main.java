package zincflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.Processor;
import zincflow.fabric.HttpServer;
import zincflow.fabric.Pipeline;
import zincflow.fabric.PipelineGraph;
import zincflow.processors.LogAttribute;
import zincflow.processors.RouteOnAttribute;
import zincflow.processors.UpdateAttribute;

import java.util.List;
import java.util.Map;

/// Phase-2 bootstrap — builds a hard-coded demo pipeline and serves the
/// minimal HTTP API on port 9092. Config.yaml-driven loading + full API
/// surface land in Phase 3.
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Pipeline pipeline = new Pipeline(demoGraph());
        int port = Integer.parseInt(System.getProperty("zincflow.port", "9092"));
        HttpServer server = new HttpServer(pipeline).start(port);
        log.info("zinc-flow-java phase 2 up — POST to http://localhost:{}/ to ingest", server.port());
        log.info("stats: GET http://localhost:{}/api/stats", server.port());
    }

    /// Hello-world pipeline for the Phase 2 demo:
    ///
    ///   ingress (LogAttribute) → router (RouteOnAttribute) → {high: UpdateAttribute[priority=elevated], low: sink}
    static PipelineGraph demoGraph() {
        Processor ingress = new LogAttribute("[ingress]");
        Processor router = new RouteOnAttribute("high: priority == urgent; low: priority == normal");
        Processor elevate = new UpdateAttribute("priority", "elevated");
        Processor tailLog = new LogAttribute("[tail]");

        Map<String, Processor> processors = Map.of(
                "ingress", ingress,
                "router", router,
                "elevate", elevate,
                "tail", tailLog);

        Map<String, Map<String, List<String>>> connections = Map.of(
                "ingress", Map.of("success", List.of("router")),
                "router",  Map.of(
                        "high",      List.of("elevate"),
                        "low",       List.of("tail"),
                        "unmatched", List.of("tail")),
                "elevate", Map.of("success", List.of("tail")));

        List<String> entryPoints = List.of("ingress");
        return new PipelineGraph(processors, connections, entryPoints);
    }
}
