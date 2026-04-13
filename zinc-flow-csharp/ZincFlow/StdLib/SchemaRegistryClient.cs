using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// Minimal Confluent-style schema registry client.
///
/// Endpoints (per Confluent REST API):
///   GET  /schemas/ids/{id}                               → {"schema": "..."}
///   GET  /subjects/{subject}/versions/latest             → {"id":N, "version":N, "subject":"...", "schema":"..."}
///   GET  /subjects/{subject}/versions/{version}          → same shape
///   POST /subjects/{subject}/versions  body {"schema":"..."} → {"id":N}
///
/// Auth: optional HTTP Basic ("user:pass" → base64 in Authorization header).
/// Caching: by ID and by (subject, version) — both are immutable in Confluent's model.
/// "latest" lookups skip the cache.
///
/// JSON parsed with hand-rolled Utf8JsonReader to stay AOT-safe without registering
/// DTOs in the source-gen context.
/// </summary>
public sealed class SchemaRegistryClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly string _baseUrl;
    private readonly ConcurrentDictionary<int, Schema> _byId = new();
    private readonly ConcurrentDictionary<string, (int Id, Schema Schema)> _bySubjectVersion = new();

    public SchemaRegistryClient(string baseUrl, string? basicAuth = null, HttpClient? http = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _http = http ?? new HttpClient();
        _ownsHttp = http is null;
        if (!string.IsNullOrEmpty(basicAuth))
        {
            var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(basicAuth));
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", token);
        }
        // Confluent recommends Accept: application/vnd.schemaregistry.v1+json — but plain JSON works.
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public async Task<Schema> GetByIdAsync(int id, CancellationToken ct = default)
    {
        if (_byId.TryGetValue(id, out var cached)) return cached;
        var json = await GetJsonAsync($"/schemas/ids/{id}", ct);
        var schemaText = ExtractStringField(json, "schema") ?? throw new InvalidOperationException($"registry response for id {id} missing 'schema' field");
        var schema = AvroSchemaJson.Parse(schemaText);
        _byId[id] = schema;
        return schema;
    }

    public async Task<(int Id, Schema Schema)> GetSubjectVersionAsync(string subject, string version, CancellationToken ct = default)
    {
        var cacheKey = $"{subject}@{version}";
        if (version != "latest" && _bySubjectVersion.TryGetValue(cacheKey, out var cached)) return cached;

        var json = await GetJsonAsync($"/subjects/{Uri.EscapeDataString(subject)}/versions/{Uri.EscapeDataString(version)}", ct);
        var id = ExtractIntField(json, "id") ?? throw new InvalidOperationException($"registry response missing 'id' for {subject}@{version}");
        var schemaText = ExtractStringField(json, "schema") ?? throw new InvalidOperationException($"registry response missing 'schema' for {subject}@{version}");
        var schema = AvroSchemaJson.Parse(schemaText);
        var result = (id, schema);
        _byId[id] = schema;
        if (version != "latest")
            _bySubjectVersion[cacheKey] = result;
        return result;
    }

    public async Task<int> RegisterAsync(string subject, Schema schema, CancellationToken ct = default)
    {
        var schemaJson = AvroSchemaJson.Emit(schema);
        // Manually compose body; the registry expects {"schema": "<escaped json string>"}.
        var bodyJson = "{\"schema\":" + JsonEncodedText.Encode(schemaJson) + "}";
        // JsonEncodedText already wraps in quotes. Verify by checking the raw text — actually
        // JsonEncodedText.Encode returns the bytes for the encoded string excluding quotes when
        // used via .Value.ToString(); to be safe we serialize via Utf8JsonWriter.
        bodyJson = ComposeRegisterBody(schemaJson);

        using var content = new StringContent(bodyJson, Encoding.UTF8, "application/vnd.schemaregistry.v1+json");
        using var resp = await _http.PostAsync($"{_baseUrl}/subjects/{Uri.EscapeDataString(subject)}/versions", content, ct);
        if (!resp.IsSuccessStatusCode)
        {
            var err = await resp.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException($"registry register failed ({(int)resp.StatusCode}): {err}");
        }
        var json = await resp.Content.ReadAsStringAsync(ct);
        var id = ExtractIntField(json, "id") ?? throw new InvalidOperationException("registry response missing 'id'");
        _byId[id] = schema;
        return id;
    }

    private static string ComposeRegisterBody(string schemaJson)
    {
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            w.WriteString("schema", schemaJson);
            w.WriteEndObject();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private async Task<string> GetJsonAsync(string path, CancellationToken ct)
    {
        using var resp = await _http.GetAsync($"{_baseUrl}{path}", ct);
        var body = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"registry GET {path} failed ({(int)resp.StatusCode}): {body}");
        return body;
    }

    // --- Hand-rolled JSON field extraction (AOT-safe, no source-gen DTOs needed) ---

    private static string? ExtractStringField(string json, string fieldName)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var reader = new Utf8JsonReader(bytes);
        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName) continue;
            if (reader.GetString() != fieldName) continue;
            reader.Read();
            return reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
        }
        return null;
    }

    private static int? ExtractIntField(string json, string fieldName)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var reader = new Utf8JsonReader(bytes);
        while (reader.Read())
        {
            if (reader.TokenType != JsonTokenType.PropertyName) continue;
            if (reader.GetString() != fieldName) continue;
            reader.Read();
            return reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out var i) ? i : null;
        }
        return null;
    }

    public void ClearCache()
    {
        _byId.Clear();
        _bySubjectVersion.Clear();
    }

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
    }
}
