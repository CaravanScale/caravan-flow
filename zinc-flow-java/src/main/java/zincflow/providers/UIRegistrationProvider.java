package zincflow.providers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/// Opt-in worker-side hook that registers this node with a central
/// UI binary and heartbeats every 30 s. The UI aggregates registered
/// workers into its node registry; self-registration is one of the
/// two supported discovery modes (static list is the other).
///
/// Config in {@code config.yaml}:
/// <pre>
/// ui:
///   register_to: http://zinc-flow-ui.ns.svc.cluster.local:9090
/// </pre>
///
/// The provider's lifecycle controls the heartbeat: {@code enable()}
/// starts the scheduled task (fire-once-immediately + every 30 s
/// afterward), {@code disable()} / {@code shutdown()} cancel it.
public final class UIRegistrationProvider implements Provider {

    private static final Logger log = LoggerFactory.getLogger(UIRegistrationProvider.class);

    public static final long DEFAULT_HEARTBEAT_SECONDS = 30;

    private final String targetUrl;
    private final Supplier<Map<String, Object>> identitySupplier;
    private final long heartbeatSeconds;
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> heartbeat;
    private volatile ComponentState state = ComponentState.DISABLED;
    private volatile long lastHeartbeatMillis;
    private volatile int lastStatus = -1;
    private volatile String lastError = "";

    public UIRegistrationProvider(String targetUrl, Supplier<Map<String, Object>> identitySupplier) {
        this(targetUrl, identitySupplier, DEFAULT_HEARTBEAT_SECONDS, HttpClient.newHttpClient());
    }

    public UIRegistrationProvider(String targetUrl,
                                  Supplier<Map<String, Object>> identitySupplier,
                                  long heartbeatSeconds,
                                  HttpClient http) {
        if (targetUrl == null || targetUrl.isEmpty())
            throw new IllegalArgumentException("targetUrl must not be blank");
        if (identitySupplier == null)
            throw new IllegalArgumentException("identitySupplier must not be null");
        if (heartbeatSeconds <= 0)
            throw new IllegalArgumentException("heartbeatSeconds must be > 0");
        this.targetUrl = targetUrl;
        this.identitySupplier = identitySupplier;
        this.heartbeatSeconds = heartbeatSeconds;
        this.http = http;
    }

    @Override public String name() { return "ui_registration"; }
    @Override public String providerType() { return "ui_registration"; }
    @Override public ComponentState state() { return state; }

    @Override
    public synchronized void enable() {
        if (state == ComponentState.ENABLED) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "zinc-flow-ui-registration");
            t.setDaemon(true);
            return t;
        });
        // Fire once immediately, then every heartbeatSeconds.
        heartbeat = scheduler.scheduleAtFixedRate(this::postOnce, 0, heartbeatSeconds, TimeUnit.SECONDS);
        state = ComponentState.ENABLED;
        log.info("ui registration enabled — target {}, heartbeat {}s", targetUrl, heartbeatSeconds);
    }

    @Override
    public synchronized void disable(int drainTimeoutSeconds) {
        stopScheduler();
        state = ComponentState.DISABLED;
    }

    @Override public void shutdown() { disable(0); }

    private void stopScheduler() {
        if (heartbeat != null) { heartbeat.cancel(false); heartbeat = null; }
        if (scheduler != null) { scheduler.shutdown(); scheduler = null; }
    }

    /// Exposed for tests + admin endpoints — most recent POST result.
    public Map<String, Object> lastOutcome() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("target", targetUrl);
        out.put("lastHeartbeatMillis", lastHeartbeatMillis);
        out.put("lastStatus", lastStatus);
        out.put("lastError", lastError);
        return out;
    }

    private void postOnce() {
        Map<String, Object> identity;
        try { identity = identitySupplier.get(); }
        catch (RuntimeException ex) {
            lastError = "identity supplier threw: " + ex;
            log.warn("ui registration: identity supplier threw {}", ex.toString());
            return;
        }
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(5))
                    .header("content-type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(identity)))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            lastHeartbeatMillis = System.currentTimeMillis();
            lastStatus = resp.statusCode();
            lastError = "";
            if (resp.statusCode() >= 400) {
                log.warn("ui registration: {} returned {}", targetUrl, resp.statusCode());
            } else {
                log.debug("ui registration: heartbeat ok ({})", resp.statusCode());
            }
        } catch (Exception ex) {
            lastHeartbeatMillis = System.currentTimeMillis();
            lastStatus = -1;
            lastError = ex.toString();
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            log.warn("ui registration: heartbeat to {} failed: {}", targetUrl, ex.toString());
        }
    }
}
