using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

/// <summary>
/// HTTP source: webhook handler for FlowFile ingestion.
/// Supports raw body and NiFi FlowFile V3 binary format.
/// </summary>
public sealed class HttpSource
{
    private readonly Fabric _fab;
    private readonly IContentStore _store;

    public HttpSource(Fabric fab, IContentStore store)
    {
        _fab = fab;
        _store = store;
    }

    public async Task HandleIngest(HttpContext ctx)
    {
        if (ctx.Request.Method != "POST")
        {
            ctx.Response.StatusCode = 405;
            await WriteJson(ctx.Response,new { error = "method not allowed" });
            return;
        }

        using var ms = new MemoryStream();
        await ctx.Request.Body.CopyToAsync(ms);
        var body = ms.ToArray();

        // V3 binary format
        if (ctx.Request.ContentType == "application/octet-stream")
        {
            var flowfiles = FlowFileV3.UnpackAll(body);
            int accepted = 0;
            foreach (var ff in flowfiles)
                if (_fab.Ingest(ff)) accepted++;

            await WriteJson(ctx.Response,new { status = "accepted", count = accepted });
            return;
        }

        // Raw ingestion
        var attrs = ExtractAttributes(ctx.Request);
        var content = ContentHelpers.MaybeOffload(_store, body);
        var flowFile = FlowFile.Rent(
            Interlocked.Increment(ref _idCounter),
            AttributeMap.FromDict(attrs),
            content,
            Environment.TickCount64);

        if (!_fab.Ingest(flowFile))
        {
            ctx.Response.StatusCode = 503;
            await WriteJson(ctx.Response,new { error = "backpressure", message = "ingest queue full" });
            return;
        }

        await WriteJson(ctx.Response,new { status = "accepted", id = flowFile.Id });
    }

    private static long _idCounter;

    public async Task HandleHealth(HttpContext ctx)
    {
        await WriteJson(ctx.Response,new { status = "healthy", dlq = _fab.GetDLQ().Count });
    }

    private static Dictionary<string, string> ExtractAttributes(HttpRequest req)
    {
        var attrs = new Dictionary<string, string>
        {
            ["http.method"] = req.Method,
            ["http.uri"] = req.Path.Value ?? "/",
            ["http.content.type"] = req.ContentType ?? "",
            ["http.host"] = req.Host.Value
        };

        // Copy X-Flow-* headers
        foreach (var header in req.Headers)
        {
            if (header.Key.StartsWith("X-Flow-", StringComparison.OrdinalIgnoreCase))
            {
                var attrKey = header.Key[7..].ToLowerInvariant();
                attrs[attrKey] = header.Value.ToString();
            }
        }

        return attrs;
    }

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    private static Task WriteJson(HttpResponse response, object value)
    {
        response.ContentType = "application/json";
        return response.WriteAsync(JsonSerializer.Serialize(value, _jsonOpts));
    }
}
