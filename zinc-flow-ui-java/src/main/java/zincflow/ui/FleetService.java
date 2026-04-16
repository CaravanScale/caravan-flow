package zincflow.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.shared.FlowSnapshot;
import zincflow.shared.Identity;
import zincflow.shared.RouteNames;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/// HTTP client that the UI uses to talk to a zinc-flow worker. Each
/// call parses the JSON response into a DTO from {@code zincflow.shared}.
/// Results are cached in an {@link AtomicReference} with a tiny TTL
/// so HTMX-driven page polling (every 2 s) doesn't multiply into
/// backend load when multiple browser tabs are open.
///
/// Slice 1 only surfaces {@link #identity()}; later slices add
/// {@code flow()}, {@code failures(n)}, {@code lineage(id)}, etc.
public final class FleetService {

    private static final Logger log = LoggerFactory.getLogger(FleetService.class);

    /// Cache TTL for every fetched resource. Keeps the worker
    /// pressure down when multiple HTMX polls hit simultaneously.
    public static final Duration CACHE_TTL = Duration.ofMillis(1500);

    private final URI workerBaseUrl;
    private final HttpClient http;
    private final ObjectMapper json;
    private final Duration requestTimeout;

    private final AtomicReference<Cached<Identity>> identityCache = new AtomicReference<>();
    private final AtomicReference<Cached<FlowSnapshot>> flowCache = new AtomicReference<>();

    public FleetService(URI workerBaseUrl) {
        this(workerBaseUrl, Duration.ofSeconds(5));
    }

    public FleetService(URI workerBaseUrl, Duration requestTimeout) {
        if (workerBaseUrl == null) throw new IllegalArgumentException("workerBaseUrl must not be null");
        this.workerBaseUrl = workerBaseUrl;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.json = new ObjectMapper();
    }

    public URI workerBaseUrl() { return workerBaseUrl; }

    public Identity identity() {
        return readCached(identityCache, RouteNames.API_IDENTITY, Identity.class);
    }

    public FlowSnapshot flow() {
        return readCached(flowCache, RouteNames.API_FLOW, FlowSnapshot.class);
    }

    /// Probe the worker — returns {@code true} when {@code /api/identity}
    /// responded 2xx within the request timeout. Used by the UI's
    /// {@code /health} handler.
    public boolean workerReachable() {
        try { identity(); return true; }
        catch (RuntimeException ex) { return false; }
    }

    // --- internals ---

    private <T> T readCached(AtomicReference<Cached<T>> ref, String path, Class<T> type) {
        Cached<T> c = ref.get();
        if (c != null && !c.expired()) return c.value;
        T fresh = fetch(path, type);
        ref.set(new Cached<>(fresh, System.nanoTime() + CACHE_TTL.toNanos()));
        return fresh;
    }

    private <T> T fetch(String path, Class<T> type) {
        URI target = workerBaseUrl.resolve(path);
        HttpRequest req = HttpRequest.newBuilder(target).timeout(requestTimeout).GET().build();
        try {
            HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException("worker " + target + " returned " + resp.statusCode());
            }
            return json.readValue(resp.body(), type);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            log.warn("worker call failed: {} — {}", target, ex.toString());
            throw new RuntimeException("worker call failed: " + target, ex);
        }
    }

    private record Cached<T>(T value, long expiresAtNanos) {
        boolean expired() { return System.nanoTime() >= expiresAtNanos; }
    }
}
