package zincflow.fabric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import zincflow.providers.SchemaRegistryProvider;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Confluent-shape REST surface over {@link SchemaRegistryProvider}.
/// Mounted under {@code /api/schema-registry/*} alongside the rest of
/// the management API so serializer clients that speak to Confluent
/// Schema Registry work verbatim against an embedded zinc-flow-java.
///
/// <h2>Mapped routes</h2>
/// <pre>
///   GET    /schemas/ids/{id}
///   GET    /subjects
///   GET    /subjects/{subject}/versions
///   GET    /subjects/{subject}/versions/{version|latest}
///   POST   /subjects/{subject}/versions          body: {"schema":"..."}
///   DELETE /subjects/{subject}
///   DELETE /subjects/{subject}/versions/{version}
/// </pre>
///
/// Responses carry
/// {@code Content-Type: application/vnd.schemaregistry.v1+json}, and
/// 4xx/5xx bodies match Confluent's {@code {"error_code", "message"}}
/// shape so any existing tooling that handles those errors works
/// against us unchanged. Mirrors zinc-flow-csharp's
/// {@code SchemaRegistryHandler}.
public final class SchemaRegistryHandler {

    public static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final SchemaRegistryProvider registry;
    private final ObjectMapper json = new ObjectMapper();

    public SchemaRegistryHandler(SchemaRegistryProvider registry) {
        if (registry == null) throw new IllegalArgumentException("registry must not be null");
        this.registry = registry;
    }

    public void mapRoutes(Javalin app) {
        app.get("/api/schema-registry/schemas/ids/{id}",                    this::getById);
        app.get("/api/schema-registry/subjects",                            this::listSubjects);
        app.get("/api/schema-registry/subjects/{subject}/versions",         this::listVersions);
        app.get("/api/schema-registry/subjects/{subject}/versions/{version}", this::getVersion);
        app.post("/api/schema-registry/subjects/{subject}/versions",        this::register);
        app.delete("/api/schema-registry/subjects/{subject}",               this::deleteSubject);
        app.delete("/api/schema-registry/subjects/{subject}/versions/{version}", this::deleteVersion);
    }

    // --- GET /schemas/ids/{id} → {"schema":"..."} ---

    private void getById(Context ctx) throws Exception {
        int id;
        try { id = Integer.parseInt(ctx.pathParam("id")); }
        catch (NumberFormatException e) {
            error(ctx, 400, 40000, "id must be an integer");
            return;
        }
        var entry = registry.getById(id);
        if (entry.isEmpty()) {
            error(ctx, 404, 40403, "schema id " + id + " not found");
            return;
        }
        write(ctx, 200, Map.of("schema", entry.get().definition()));
    }

    // --- GET /subjects → ["a","b","c"] ---

    private void listSubjects(Context ctx) throws Exception {
        writeRaw(ctx, 200, json.writeValueAsBytes(registry.listSubjects()));
    }

    // --- GET /subjects/{subject}/versions → [1,2,3] ---

    private void listVersions(Context ctx) throws Exception {
        String subject = ctx.pathParam("subject");
        List<Integer> versions = registry.listVersions(subject);
        if (versions.isEmpty()) {
            error(ctx, 404, 40401, "subject '" + subject + "' not found");
            return;
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(versions));
    }

    // --- GET /subjects/{subject}/versions/{version|latest} → full payload ---

    private void getVersion(Context ctx) throws Exception {
        String subject = ctx.pathParam("subject");
        String version = ctx.pathParam("version");
        var entry = "latest".equals(version)
                ? registry.latest(subject)
                : parseVersion(ctx, version).flatMap(v -> registry.getEntry(subject, v));
        if (entry.isEmpty()) {
            // Distinguish unknown subject vs unknown version — Confluent does.
            if (!registry.listSubjects().contains(subject)) {
                error(ctx, 404, 40401, "subject '" + subject + "' not found");
            } else {
                error(ctx, 404, 40402, "subject '" + subject + "' version " + version + " not found");
            }
            return;
        }
        var s = entry.get();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("subject", s.subject());
        body.put("version", s.version());
        body.put("id", s.id());
        body.put("schema", s.definition());
        write(ctx, 200, body);
    }

    private java.util.Optional<Integer> parseVersion(Context ctx, String raw) {
        try {
            return java.util.Optional.of(Integer.parseInt(raw));
        } catch (NumberFormatException e) {
            return java.util.Optional.empty();
        }
    }

    // --- POST /subjects/{subject}/versions  body {"schema":"..."}  → {"id":N} ---

    private void register(Context ctx) throws Exception {
        String subject = ctx.pathParam("subject");
        String body = ctx.body();
        String schemaDef;
        try {
            JsonNode root = json.readTree(body);
            JsonNode schemaNode = root.get("schema");
            if (schemaNode == null || !schemaNode.isTextual()) {
                error(ctx, 422, 42201, "request body missing 'schema' string field");
                return;
            }
            schemaDef = schemaNode.asText();
        } catch (Exception ex) {
            error(ctx, 422, 42201, "request body is not valid JSON: " + ex.getMessage());
            return;
        }

        try {
            var registered = registry.register(subject, schemaDef);
            write(ctx, 200, Map.of("id", registered.id()));
        } catch (IllegalArgumentException ex) {
            error(ctx, 422, 42202, "register failed: " + ex.getMessage());
        } catch (RuntimeException ex) {
            error(ctx, 500, 50001, "register failed: " + ex.getMessage());
        }
    }

    // --- DELETE /subjects/{subject} → [1,2,3] of removed versions ---

    private void deleteSubject(Context ctx) throws Exception {
        String subject = ctx.pathParam("subject");
        List<Integer> removed = registry.deleteSubject(subject);
        if (removed.isEmpty()) {
            error(ctx, 404, 40401, "subject '" + subject + "' not found");
            return;
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(removed));
    }

    // --- DELETE /subjects/{subject}/versions/{version} → version number ---

    private void deleteVersion(Context ctx) throws Exception {
        String subject = ctx.pathParam("subject");
        int version;
        try { version = Integer.parseInt(ctx.pathParam("version")); }
        catch (NumberFormatException e) {
            error(ctx, 400, 40000, "version must be an integer");
            return;
        }
        boolean deleted = registry.deleteVersion(subject, version);
        if (!deleted) {
            error(ctx, 404, 40402, "subject '" + subject + "' version " + version + " not found");
            return;
        }
        writeRaw(ctx, 200, json.writeValueAsBytes(version));
    }

    // --- helpers ---

    private void write(Context ctx, int status, Object body) throws Exception {
        ctx.status(status)
           .contentType(CONTENT_TYPE)
           .result(json.writeValueAsBytes(body));
    }

    private void writeRaw(Context ctx, int status, byte[] body) {
        ctx.status(status).contentType(CONTENT_TYPE).result(body);
    }

    private void error(Context ctx, int status, int errorCode, String message) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error_code", errorCode);
        body.put("message", message);
        write(ctx, status, body);
    }
}
