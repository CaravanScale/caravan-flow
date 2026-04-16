package zincflow.ui;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.pebble.loader.ClasspathLoader;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/// Boot entry for zinc-flow-ui. Single-worker Phase 1 MVP: serves a
/// browser UI that consumes a zinc-flow worker's management API.
///
/// <h2>Configuration resolution order</h2>
/// CLI arg → env var → system property → default. Same pattern the
/// worker uses.
/// <ul>
///   <li>{@code --worker URL} / {@code ZINCFLOW_WORKER_URL} /
///       {@code zincflow.ui.workerUrl} / {@code http://localhost:9092}</li>
///   <li>{@code --port N} / {@code ZINCFLOW_UI_PORT} /
///       {@code zincflow.ui.port} / {@code 9090}</li>
/// </ul>
public final class UiMain {

    private static final Logger log = LoggerFactory.getLogger(UiMain.class);

    public static final String DEFAULT_WORKER = "http://localhost:9092";
    public static final int    DEFAULT_PORT   = 9090;

    public static final String ENV_WORKER   = "ZINCFLOW_WORKER_URL";
    public static final String ENV_PORT     = "ZINCFLOW_UI_PORT";
    public static final String PROP_WORKER  = "zincflow.ui.workerUrl";
    public static final String PROP_PORT    = "zincflow.ui.port";

    private final FleetService fleet;
    private final PebbleEngine pebble;
    private Javalin app;
    private volatile int boundPort = -1;

    public UiMain(URI workerBaseUrl) {
        this.fleet = new FleetService(workerBaseUrl);
        this.pebble = new PebbleEngine.Builder()
                .loader(new ClasspathLoader())
                .cacheActive(false) // dev-friendly; templates reload on change
                .build();
    }

    public static void main(String[] args) {
        Config cfg = Config.resolve(args);
        UiMain ui = new UiMain(cfg.workerUrl);
        ui.start(cfg.port);
        log.info("zinc-flow-ui up — port {}, worker {}", ui.boundPort, cfg.workerUrl);
        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("zinc-flow-ui-shutdown").unstarted(() -> {
            log.info("shutdown signal — stopping ui");
            ui.stop();
        }));
    }

    public UiMain start(int port) {
        app = Javalin.create(javalin -> {
                    QueuedThreadPool qtp = new QueuedThreadPool();
                    qtp.setName("zinc-flow-ui-jetty");
                    qtp.setVirtualThreadsExecutor(Executors.newVirtualThreadPerTaskExecutor());
                    javalin.jetty.threadPool = qtp;
                    javalin.staticFiles.add(cfg -> { cfg.hostedPath = "/static"; cfg.directory = "/static"; });
                })
                .get(UiRoutes.ROOT,   ctx -> ctx.redirect(UiRoutes.FLOW))
                .get(UiRoutes.HEALTH, this::handleHealth)
                .get(UiRoutes.FLOW,   this::handleFlowPlaceholder)
                .start(port);
        boundPort = app.port();
        return this;
    }

    public int port() {
        if (boundPort < 0) throw new IllegalStateException("ui not started");
        return boundPort;
    }

    public void stop() {
        if (app != null) {
            app.stop();
            app = null;
            boundPort = -1;
        }
    }

    public FleetService fleet() { return fleet; }

    // --- Handlers ---

    private void handleHealth(Context ctx) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "healthy");
        out.put("workerUrl", fleet.workerBaseUrl().toString());
        out.put("workerReachable", fleet.workerReachable());
        ctx.json(out);
    }

    /// Phase 1 placeholder for the /flow view — the real implementation
    /// lands in slice 2. Kept here so /flow → 200 instead of 404 while
    /// slice 1's shell is in place.
    private void handleFlowPlaceholder(Context ctx) throws Exception {
        Map<String, Object> model = new LinkedHashMap<>();
        model.put("workerUrl", fleet.workerBaseUrl().toString());
        try { model.put("identity", fleet.identity()); }
        catch (RuntimeException ex) { model.put("identityError", ex.getMessage()); }
        ctx.contentType("text/html; charset=utf-8").result(render("flow.peb", model));
    }

    private String render(String template, Map<String, Object> model) throws Exception {
        PebbleTemplate t = pebble.getTemplate("templates/" + template);
        StringWriter sw = new StringWriter();
        t.evaluate(sw, model);
        return sw.toString();
    }

    /// Resolved UI config. Exposed as a record so tests can build one
    /// without round-tripping through argv parsing.
    public record Config(URI workerUrl, int port) {

        public static Config resolve(String[] args) {
            String worker = defaultFor(args, "--worker", ENV_WORKER, PROP_WORKER, DEFAULT_WORKER);
            String port   = defaultFor(args, "--port",   ENV_PORT,   PROP_PORT,   Integer.toString(DEFAULT_PORT));
            return new Config(URI.create(worker), Integer.parseInt(port));
        }

        private static String defaultFor(String[] args, String flag, String env, String prop, String fallback) {
            for (int i = 0; i + 1 < args.length; i++) {
                if (args[i].equals(flag)) return args[i + 1];
            }
            String e = System.getenv(env);
            if (e != null && !e.isEmpty()) return e;
            String p = System.getProperty(prop);
            if (p != null && !p.isEmpty()) return p;
            return fallback;
        }
    }
}
