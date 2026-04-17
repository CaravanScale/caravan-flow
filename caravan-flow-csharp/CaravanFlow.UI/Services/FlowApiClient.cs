using System.Net.Http.Json;
using System.Text.Json;

namespace CaravanFlow.UI.Services;

/// <summary>
/// Typed wrapper over the worker's management API. The worker exposes
/// its API at the same origin that serves this WASM bundle, so the
/// HttpClient base address is whatever <c>HostEnvironment.BaseAddress</c>
/// resolved to — no separate config.
///
/// DTOs are intentionally kept as <see cref="JsonElement"/> /
/// dictionaries rather than typed records for the MVP — the worker's
/// types live in the server-side CaravanFlow project today, and
/// pulling that into the WASM bundle would balloon it. Once the UI
/// stabilizes, a CaravanFlow.Shared project will extract the pure DTOs
/// and this client can switch to strongly typed deserialization.
/// </summary>
public sealed class FlowApiClient
{
    private readonly HttpClient _http;

    public FlowApiClient(HttpClient http) => _http = http;

    // --- Graph / runtime state ---

    public Task<JsonElement> GetFlowAsync(CancellationToken ct = default)
        => GetJsonAsync("api/flow", ct);

    public Task<JsonElement> GetStatsAsync(CancellationToken ct = default)
        => GetJsonAsync("api/stats", ct);

    public Task<JsonElement> GetProcessorStatsAsync(CancellationToken ct = default)
        => GetJsonAsync("api/processor-stats", ct);

    public Task<JsonElement> GetRegistryAsync(CancellationToken ct = default)
        => GetJsonAsync("api/registry", ct);

    // --- Processor CRUD ---

    public Task<HttpResponseMessage> AddProcessorAsync(object body, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/processors/add", body, ct);

    public Task<HttpResponseMessage> RemoveProcessorAsync(string name, CancellationToken ct = default)
        => _http.DeleteAsync($"api/processors/remove?name={Uri.EscapeDataString(name)}", ct);

    public Task<HttpResponseMessage> UpdateProcessorConfigAsync(string name, object body, CancellationToken ct = default)
        => _http.PutAsJsonAsync($"api/processors/{Uri.EscapeDataString(name)}/config", body, ct);

    public Task<HttpResponseMessage> EnableProcessorAsync(string name, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/processors/enable", new { name }, ct);

    public Task<HttpResponseMessage> DisableProcessorAsync(string name, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/processors/disable", new { name }, ct);

    // --- Connection edits ---

    public Task<HttpResponseMessage> AddConnectionAsync(string from, string relationship, string to, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/connections", new { from, relationship, to }, ct);

    public Task<HttpResponseMessage> RemoveConnectionAsync(string from, string relationship, string to, CancellationToken ct = default)
    {
        var req = new HttpRequestMessage(HttpMethod.Delete, "api/connections")
        {
            Content = JsonContent.Create(new { from, relationship, to }),
        };
        return _http.SendAsync(req, ct);
    }

    public Task<HttpResponseMessage> SetConnectionsAsync(string from, object relationships, CancellationToken ct = default)
        => _http.PutAsJsonAsync($"api/connections/{Uri.EscapeDataString(from)}", relationships, ct);

    public Task<HttpResponseMessage> SetEntryPointsAsync(IEnumerable<string> names, CancellationToken ct = default)
        => _http.PutAsJsonAsync("api/entrypoints", new { names }, ct);

    // --- Providers ---

    public Task<JsonElement> GetProvidersAsync(CancellationToken ct = default)
        => GetJsonAsync("api/providers", ct);

    public Task<HttpResponseMessage> UpdateProviderConfigAsync(string name, object body, CancellationToken ct = default)
        => _http.PutAsJsonAsync($"api/providers/{Uri.EscapeDataString(name)}/config", body, ct);

    // --- Provenance / lineage / errors ---

    public Task<JsonElement> GetProvenanceRecentAsync(int n = 50, CancellationToken ct = default)
        => GetJsonAsync($"api/provenance?n={n}", ct);

    public Task<JsonElement> GetProvenanceByIdAsync(long id, CancellationToken ct = default)
        => GetJsonAsync($"api/provenance/{id}", ct);

    public Task<JsonElement> GetProvenanceFailuresAsync(int n = 50, CancellationToken ct = default)
        => GetJsonAsync($"api/provenance/failures?n={n}", ct);

    // --- Settings / overlays ---

    public Task<JsonElement> GetOverlaysAsync(CancellationToken ct = default)
        => GetJsonAsync("api/overlays", ct);

    public Task<HttpResponseMessage> WriteSecretsAsync(object body, CancellationToken ct = default)
        => _http.PutAsJsonAsync("api/overlays/secrets", body, ct);

    // --- Version control ---

    public Task<JsonElement> GetVcStatusAsync(CancellationToken ct = default)
        => GetJsonAsync("api/vc/status", ct);

    public Task<HttpResponseMessage> VcCommitAsync(string message, string? path = null, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/vc/commit", new { message, path }, ct);

    public Task<HttpResponseMessage> VcPushAsync(CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/vc/push", new { }, ct);

    // --- Flow save ---

    public Task<HttpResponseMessage> SaveFlowAsync(string? message = null, bool push = true, CancellationToken ct = default)
        => _http.PostAsJsonAsync("api/flow/save", new { message, push }, ct);

    // --- Metrics ---

    public async Task<string> GetMetricsRawAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync("metrics", ct);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadAsStringAsync(ct);
    }

    // --- Internal ---

    private async Task<JsonElement> GetJsonAsync(string url, CancellationToken ct)
    {
        using var resp = await _http.GetAsync(url, ct);
        resp.EnsureSuccessStatusCode();
        await using var stream = await resp.Content.ReadAsStreamAsync(ct);
        using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        return doc.RootElement.Clone();
    }
}
