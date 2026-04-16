package zincflow.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.Content;
import zincflow.core.FlowFile;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.core.RawContent;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

/// Sink-style processor that POSTs (or PUTs) the FlowFile's RawContent
/// to a URL using the JDK's standard HttpClient. Returns the FlowFile
/// on "success" when the response is 2xx, with the status code and
/// response size added as attributes. Non-2xx or transport errors
/// route to "failure."
///
/// Uses only java.net.http — no transitive dependencies, HTTP/2 capable
/// by default.
public final class PutHTTP implements Processor {

    private static final Logger log = LoggerFactory.getLogger(PutHTTP.class);

    private final URI endpoint;
    private final String method;
    private final Duration timeout;
    private final String contentType;
    private final HttpClient client;

    public PutHTTP(String endpoint) { this(endpoint, "POST", Duration.ofSeconds(30), "application/octet-stream"); }

    public PutHTTP(String endpoint, String method, Duration timeout, String contentType) {
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException("PutHTTP: endpoint must not be blank");
        }
        this.endpoint = URI.create(endpoint);
        this.method = switch (method.toUpperCase()) {
            case "POST", "PUT", "PATCH" -> method.toUpperCase();
            default -> throw new IllegalArgumentException("PutHTTP: method must be POST/PUT/PATCH, got " + method);
        };
        this.timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
        this.contentType = contentType == null ? "application/octet-stream" : contentType;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        Content content = ff.content();
        if (!(content instanceof RawContent raw)) {
            return ProcessorResult.failure(
                    "PutHTTP: expected RawContent, got " + content.getClass().getSimpleName(), ff);
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(timeout)
                .header("Content-Type", contentType)
                .method(method, BodyPublishers.ofByteArray(raw.bytes()))
                .build();
        try {
            HttpResponse<byte[]> response = client.send(request, BodyHandlers.ofByteArray());
            int status = response.statusCode();
            FlowFile withMeta = ff
                    .withAttribute("puthttp.status", String.valueOf(status))
                    .withAttribute("puthttp.response.size", String.valueOf(response.body().length));
            if (status >= 200 && status < 300) {
                return ProcessorResult.single(withMeta);
            }
            return ProcessorResult.failure("PutHTTP: non-2xx status " + status, withMeta);
        } catch (java.io.IOException | InterruptedException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("PutHTTP: transport error to {}: {}", endpoint, ex.toString());
            return ProcessorResult.failure("PutHTTP: " + ex.getMessage(), ff);
        }
    }
}
