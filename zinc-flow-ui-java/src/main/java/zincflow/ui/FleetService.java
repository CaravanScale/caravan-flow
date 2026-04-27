package zincflow.ui;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.shared.FlowSnapshot;
import zincflow.shared.Identity;
import zincflow.shared.OverlayInfo;
import zincflow.shared.ProvenanceEvent;
import zincflow.shared.RouteNames;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
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

    /// Default failure inbox size when callers don't pass one. Matches
    /// the NiFi bulletin-style window the UI shows.
    public static final int DEFAULT_FAILURES_N = 50;

    /// Cache TTL for every fetched resource. Keeps the worker
    /// pressure down when multiple HTMX polls hit simultaneously.
    public static final Duration CACHE_TTL = Duration.ofMillis(1500);

    private final URI workerBaseUrl;
    private final HttpClient http;
    private final ObjectMapper json;
    private final Duration requestTimeout;

    private final AtomicReference<Cached<Identity>> identityCache = new AtomicReference<>();
    private final AtomicReference<Cached<FlowSnapshot>> flowCache = new AtomicReference<>();
    /// Shared cache for the default (DEFAULT_FAILURES_N) failures fetch
    /// — the common path from HTMX polling. Non-default n values bypass
    /// the cache so an on-demand larger pull is never served stale data.
    private final AtomicReference<Cached<List<ProvenanceEvent>>> failuresCache = new AtomicReference<>();
    private final AtomicReference<Cached<OverlayInfo>> overlaysCache = new AtomicReference<>();

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

    /// Recent FAILED provenance events from the worker, newest first.
    /// Only the default-size fetch is cached — callers requesting a
    /// different {@code n} (e.g. a deeper ad-hoc pull) always see a
    /// fresh response.
    public List<ProvenanceEvent> failures() {
        return failures(DEFAULT_FAILURES_N);
    }

    public List<ProvenanceEvent> failures(int n) {
        if (n <= 0) throw new IllegalArgumentException("n must be positive, got " + n);
        String path = RouteNames.API_PROVENANCE_FAILURES + "?n=" + n;
        if (n == DEFAULT_FAILURES_N) {
            return readCachedList(failuresCache, path, ProvenanceEvent.class);
        }
        return fetchList(path, ProvenanceEvent.class);
    }

    /// Every provenance event recorded for a single FlowFile, in the
    /// order the provider returns them (chronological per source). Not
    /// cached — detail pages are user-triggered, so a fresh read per
    /// hit keeps the view honest.
    public List<ProvenanceEvent> lineage(long flowFileId) {
        return fetchList(RouteNames.API_PROVENANCE_BY_ID + flowFileId, ProvenanceEvent.class);
    }

    /// Current overlay stack from the worker — base YAML plus any
    /// role-specific layers and secrets — as assembled by
    /// {@code ConfigLoader}. Read-only surface; editing will land in
    /// Phase 3.
    public OverlayInfo overlays() {
        return readCached(overlaysCache, RouteNames.API_OVERLAYS, OverlayInfo.class);
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
        T fresh = fetch(path, json.constructType(type));
        ref.set(new Cached<>(fresh, System.nanoTime() + CACHE_TTL.toNanos()));
        return fresh;
    }

    private <E> List<E> readCachedList(AtomicReference<Cached<List<E>>> ref, String path, Class<E> elementType) {
        Cached<List<E>> c = ref.get();
        if (c != null && !c.expired()) return c.value;
        List<E> fresh = fetchList(path, elementType);
        ref.set(new Cached<>(fresh, System.nanoTime() + CACHE_TTL.toNanos()));
        return fresh;
    }

    private <E> List<E> fetchList(String path, Class<E> elementType) {
        JavaType type = json.getTypeFactory().constructCollectionType(List.class, elementType);
        return fetch(path, type);
    }

    @SuppressWarnings("unchecked")
    private <T> T fetch(String path, JavaType type) {
        URI target = workerBaseUrl.resolve(path);
        HttpRequest req = HttpRequest.newBuilder(target).timeout(requestTimeout).GET().build();
        try {
            HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() / 100 != 2) {
                throw new RuntimeException("worker " + target + " returned " + resp.statusCode());
            }
            return (T) json.readValue(resp.body(), type);
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
