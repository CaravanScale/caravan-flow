package zincflow.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ContentResolver;
import zincflow.core.ContentStore;
import zincflow.core.FlowFile;
import zincflow.core.FlowFileAttributes;
import zincflow.core.Processor;
import zincflow.core.ProcessorResult;
import zincflow.fabric.FlowFileV3;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

/// Sink-style processor that POSTs / PUTs / PATCHes the FlowFile's
/// content to a URL via the JDK's {@link HttpClient}. 2xx → success;
/// non-2xx or transport error → failure.
///
/// {@code format=v3} wraps the body with NiFi FlowFile V3 framing and
/// sets {@code application/flowfile-v3} as the Content-Type so the
/// downstream receiver can round-trip the original attributes.
/// Claim-backed content resolves through the supplied
/// {@link ContentStore}.
public final class PutHTTP implements Processor {

    private static final Logger log = LoggerFactory.getLogger(PutHTTP.class);

    private final URI endpoint;
    private final String method;
    private final Duration timeout;
    private final String contentType;
    private final boolean v3;
    private final ContentStore store;
    private final HttpClient client;

    public PutHTTP(String endpoint) {
        this(endpoint, "POST", Duration.ofSeconds(30), "application/octet-stream", "raw", null);
    }

    public PutHTTP(String endpoint, String method, Duration timeout, String contentType) {
        this(endpoint, method, timeout, contentType, "raw", null);
    }

    public PutHTTP(String endpoint, String method, Duration timeout, String contentType,
                   String format, ContentStore store) {
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException("PutHTTP: endpoint must not be blank");
        }
        this.endpoint = URI.create(endpoint);
        this.method = switch (method.toUpperCase()) {
            case "POST", "PUT", "PATCH" -> method.toUpperCase();
            default -> throw new IllegalArgumentException("PutHTTP: method must be POST/PUT/PATCH, got " + method);
        };
        this.timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
        this.v3 = "v3".equalsIgnoreCase(format);
        this.contentType = this.v3 ? "application/flowfile-v3"
                : (contentType == null ? "application/octet-stream" : contentType);
        this.store = store;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .version(HttpClient.Version.HTTP_2)
                .build();
    }

    @Override
    public ProcessorResult process(FlowFile ff) {
        ContentResolver.Resolution resolved = ContentResolver.resolve(ff.content(), store);
        if (!resolved.ok()) {
            return ProcessorResult.failure("PutHTTP: " + resolved.error(), ff);
        }
        byte[] body = v3 ? FlowFileV3.pack(ff, resolved.bytes()) : resolved.bytes();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(timeout)
                .header("Content-Type", contentType)
                .method(method, BodyPublishers.ofByteArray(body))
                .build();
        try {
            HttpResponse<byte[]> response = client.send(request, BodyHandlers.ofByteArray());
            int status = response.statusCode();
            FlowFile withMeta = ff
                    .withAttribute(FlowFileAttributes.PUTHTTP_STATUS, String.valueOf(status))
                    .withAttribute(FlowFileAttributes.PUTHTTP_RESPONSE_SIZE, String.valueOf(response.body().length));
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
