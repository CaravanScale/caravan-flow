using System.Text;
using System.Text.Json;
using ZincFlow.Core;
using ZincFlow.StdLib;

namespace ZincFlow.Fabric;

/// <summary>
/// Confluent-shape REST endpoints for the embedded schema registry.
/// Mounted under /api/schema-registry/* on the management port.
///
/// JSON parsed/built with hand-rolled Utf8JsonReader/Writer to stay AOT-clean —
/// no DTO classes registered in ZincJsonContext.
/// 4xx response bodies match Confluent's error_code/message shape so any
/// existing tooling that handles those errors works against us.
/// </summary>
public sealed class SchemaRegistryHandler
{
    private readonly EmbeddedSchemaRegistry _registry;

    public SchemaRegistryHandler(EmbeddedSchemaRegistry registry) => _registry = registry;

    public void MapRoutes(WebApplication app)
    {
        app.MapGet("/api/schema-registry/schemas/ids/{id:int}", GetById);
        app.MapGet("/api/schema-registry/subjects", ListSubjects);
        app.MapGet("/api/schema-registry/subjects/{subject}/versions", ListVersions);
        app.MapGet("/api/schema-registry/subjects/{subject}/versions/{version}", GetSubjectVersion);
        app.MapPost("/api/schema-registry/subjects/{subject}/versions", (Delegate)RegisterSchema);
        app.MapDelete("/api/schema-registry/subjects/{subject}", DeleteSubject);
        app.MapDelete("/api/schema-registry/subjects/{subject}/versions/{version:int}", DeleteVersion);
    }

    // --- GET /schemas/ids/{id} → {"schema":"..."} ---

    private IResult GetById(int id)
    {
        var entry = _registry.GetEntryByIdInternal(id);
        if (entry is null) return JsonError(404, 40403, $"schema id {id} not found");
        return JsonContent(BuildJson(("schema", AvroSchemaJson.Emit(entry.Value.Schema))));
    }

    // --- GET /subjects → ["a","b","c"] ---

    private IResult ListSubjects() => JsonContent(BuildStringArray(_registry.ListSubjects()));

    // --- GET /subjects/{subject}/versions → [1,2,3] ---

    private IResult ListVersions(string subject)
    {
        var versions = _registry.ListVersions(subject);
        if (versions.Count == 0) return JsonError(404, 40401, $"subject '{subject}' not found");
        return JsonContent(BuildIntArray(versions));
    }

    // --- GET /subjects/{subject}/versions/{version|latest} → full payload ---

    private IResult GetSubjectVersion(string subject, string version)
    {
        EmbeddedSchemaRegistry.VersionEntry entry;
        if (version == "latest")
        {
            if (!_registry.TryGetLatest(subject, out entry))
                return JsonError(404, 40401, $"subject '{subject}' not found");
        }
        else if (int.TryParse(version, out var v))
        {
            if (!_registry.TryGetEntry(subject, v, out entry))
                return JsonError(404, 40402, $"subject '{subject}' version {v} not found");
        }
        else
        {
            return JsonError(400, 40000, "version must be an integer or 'latest'");
        }
        return JsonContent(BuildJson(
            ("subject", subject),
            ("version", entry.Version),
            ("id", entry.Id),
            ("schema", AvroSchemaJson.Emit(entry.Schema))));
    }

    // --- POST /subjects/{subject}/versions  body {"schema":"..."}  → {"id":N} ---

    private async Task<IResult> RegisterSchema(string subject, HttpContext ctx)
    {
        string body;
        using (var sr = new StreamReader(ctx.Request.Body, Encoding.UTF8))
            body = await sr.ReadToEndAsync();

        var schemaJson = ExtractSchemaField(body);
        if (schemaJson is null) return JsonError(422, 42201, "request body missing 'schema' string field");

        Schema schema;
        try { schema = AvroSchemaJson.Parse(schemaJson); }
        catch (Exception ex) { return JsonError(422, 42202, $"invalid Avro schema: {ex.Message}"); }

        int id;
        try { id = await _registry.RegisterAsync(subject, schema); }
        catch (Exception ex) { return JsonError(500, 50001, $"register failed: {ex.Message}"); }

        return JsonContent(BuildJson(("id", id)));
    }

    // --- DELETE /subjects/{subject} → list of deleted versions ---

    private IResult DeleteSubject(string subject)
    {
        var versions = _registry.ListVersions(subject);
        if (versions.Count == 0) return JsonError(404, 40401, $"subject '{subject}' not found");
        _registry.DeleteSubject(subject);
        return JsonContent(BuildIntArray(versions));
    }

    // --- DELETE /subjects/{subject}/versions/{version} → version deleted ---

    private IResult DeleteVersion(string subject, int version)
    {
        if (!_registry.DeleteVersion(subject, version))
            return JsonError(404, 40402, $"subject '{subject}' version {version} not found");
        return JsonContent(version.ToString());
    }

    // --- Hand-rolled JSON helpers ---

    private static IResult JsonContent(string json)
        => Results.Content(json, "application/vnd.schemaregistry.v1+json");

    private static IResult JsonError(int statusCode, int errorCode, string message)
    {
        var body = BuildJson(("error_code", errorCode), ("message", message));
        return Results.Content(body, "application/vnd.schemaregistry.v1+json", statusCode: statusCode);
    }

    private static string BuildJson(params (string Key, object Value)[] pairs)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            foreach (var (k, v) in pairs)
            {
                switch (v)
                {
                    case int i: w.WriteNumber(k, i); break;
                    case long l: w.WriteNumber(k, l); break;
                    case string s: w.WriteString(k, s); break;
                    case bool b: w.WriteBoolean(k, b); break;
                    default: w.WriteString(k, v.ToString() ?? ""); break;
                }
            }
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static string BuildStringArray(List<string> items)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartArray();
            foreach (var s in items) w.WriteStringValue(s);
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static string BuildIntArray(List<int> items)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartArray();
            foreach (var i in items) w.WriteNumberValue(i);
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    /// <summary>Extracts the "schema" string field from a Confluent register body.</summary>
    private static string? ExtractSchemaField(string body)
    {
        var bytes = Encoding.UTF8.GetBytes(body);
        var reader = new Utf8JsonReader(bytes);
        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName) continue;
            if (reader.GetString() != "schema") continue;
            reader.Read();
            return reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
        }
        return null;
    }
}
