using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ZincFlow.Core;

namespace ZincFlow.Fabric;

// --- Shared helpers for source connectors ---

internal static class SourceHelpers
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        TypeInfoResolver = new DefaultJsonTypeInfoResolver()
    };

    internal static Task WriteJson(HttpResponse response, object value)
    {
        response.ContentType = "application/json";
        return response.WriteAsync(JsonSerializer.Serialize(value, JsonOpts));
    }
}

/// <summary>
/// File source connector: watches a directory and ingests new files as FlowFiles.
/// Files are moved to a "processed" subdirectory after ingestion.
/// </summary>
public sealed class GetFile : IConnectorSource
{
    public string Name { get; }
    public string SourceType => "GetFile";
    public bool IsRunning { get; private set; }

    private readonly string _inputDir;
    private readonly string _processedDir;
    private readonly string _pattern;
    private readonly int _pollIntervalMs;
    private readonly IContentStore _store;
    private Func<FlowFile, bool> _ingest = null!;
    private CancellationTokenSource? _cts;

    public GetFile(string name, string inputDir, string pattern, int pollIntervalMs, IContentStore store)
    {
        Name = name;
        _inputDir = inputDir;
        _processedDir = Path.Combine(inputDir, ".processed");
        _pattern = string.IsNullOrEmpty(pattern) ? "*" : pattern;
        _pollIntervalMs = pollIntervalMs > 0 ? pollIntervalMs : 1000;
        _store = store;
    }

    public void Start(Func<FlowFile, bool> ingest, CancellationToken ct)
    {
        _ingest = ingest;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        IsRunning = true;

        Directory.CreateDirectory(_inputDir);
        Directory.CreateDirectory(_processedDir);

        _ = Task.Run(() => PollLoop(_cts.Token), _cts.Token);
    }

    public void Stop()
    {
        IsRunning = false;
        _cts?.Cancel();
    }

    private async Task PollLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && IsRunning)
        {
            try
            {
                var files = Directory.GetFiles(_inputDir, _pattern);
                foreach (var filePath in files)
                {
                    if (ct.IsCancellationRequested) break;

                    var fileName = Path.GetFileName(filePath);
                    try
                    {
                        var data = await File.ReadAllBytesAsync(filePath, ct);
                        var content = ContentHelpers.MaybeOffload(_store, data);
                        var attrs = new Dictionary<string, string>
                        {
                            ["filename"] = fileName,
                            ["path"] = filePath,
                            ["size"] = data.Length.ToString(),
                            ["source"] = Name
                        };
                        var ff = FlowFile.CreateWithContent(content, attrs);

                        if (_ingest(ff))
                        {
                            // Move to processed
                            var dest = Path.Combine(_processedDir, fileName);
                            if (File.Exists(dest)) File.Delete(dest);
                            File.Move(filePath, dest);
                        }
                        // else: backpressure — leave file for next poll
                    }
                    catch (IOException)
                    {
                        // File in use or disappeared — skip, retry next poll
                    }
                }
            }
            catch (DirectoryNotFoundException)
            {
                Directory.CreateDirectory(_inputDir);
            }

            try { await Task.Delay(_pollIntervalMs, ct); }
            catch (OperationCanceledException) { break; }
        }

        IsRunning = false;
    }
}

/// <summary>
/// ListenHTTP source connector: starts its own HTTP server on a dedicated port
/// and accepts FlowFile ingestion (raw body or NiFi V3 binary).
/// Independent from the main server — can be started/stopped via API.
/// </summary>
public sealed class ListenHTTP : IConnectorSource
{
    public string Name { get; }
    public string SourceType => "ListenHTTP";
    public bool IsRunning { get; private set; }

    private readonly int _port;
    private readonly string _path;
    private readonly IContentStore _store;
    private Func<FlowFile, bool> _ingest = null!;
    private WebApplication? _app;
    private CancellationTokenSource? _cts;
    private static long _idCounter;

    public ListenHTTP(string name, int port, string path, IContentStore store)
    {
        Name = name;
        _port = port;
        _path = string.IsNullOrEmpty(path) ? "/" : path;
        _store = store;
    }

    public void Start(Func<FlowFile, bool> ingest, CancellationToken ct)
    {
        _ingest = ingest;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        IsRunning = true;

        _ = Task.Run(async () =>
        {
            try
            {
                var builder = WebApplication.CreateBuilder();
                builder.Logging.ClearProviders();
                builder.WebHost.UseUrls($"http://0.0.0.0:{_port}");
                builder.Services.ConfigureHttpJsonOptions(options =>
                {
                    options.SerializerOptions.TypeInfoResolverChain.Insert(0,
                        new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver());
                });

                _app = builder.Build();
                _app.Map(_path, (RequestDelegate)HandleRequest);
                _app.Map("/health", (RequestDelegate)HandleHealth);

                Console.WriteLine($"[listen-http] {Name} listening on :{_port}{_path}");
                await _app.RunAsync(_cts.Token);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[listen-http] {Name} error: {ex.Message}");
            }
            finally
            {
                IsRunning = false;
            }
        }, _cts.Token);
    }

    public void Stop()
    {
        IsRunning = false;
        _cts?.Cancel();
        _app?.StopAsync().Wait(TimeSpan.FromSeconds(5));
    }

    private async Task HandleRequest(HttpContext ctx)
    {
        if (ctx.Request.Method != "POST")
        {
            ctx.Response.StatusCode = 405;
            await SourceHelpers.WriteJson(ctx.Response, new { error = "method not allowed" });
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
                if (_ingest(ff)) accepted++;
            await SourceHelpers.WriteJson(ctx.Response, new { status = "accepted", count = accepted, source = Name });
            return;
        }

        // Raw ingestion
        var attrs = new Dictionary<string, string>
        {
            ["http.method"] = ctx.Request.Method,
            ["http.uri"] = ctx.Request.Path.Value ?? "/",
            ["http.content.type"] = ctx.Request.ContentType ?? "",
            ["source"] = Name
        };

        foreach (var header in ctx.Request.Headers)
        {
            if (header.Key.StartsWith("X-Flow-", StringComparison.OrdinalIgnoreCase))
                attrs[header.Key[7..].ToLowerInvariant()] = header.Value.ToString();
        }

        var content = ContentHelpers.MaybeOffload(_store, body);
        var flowFile = FlowFile.Rent(
            Interlocked.Increment(ref _idCounter),
            AttributeMap.FromDict(attrs),
            content,
            Environment.TickCount64);

        if (!_ingest(flowFile))
        {
            ctx.Response.StatusCode = 503;
            await SourceHelpers.WriteJson(ctx.Response, new { error = "backpressure", source = Name });
            return;
        }

        await SourceHelpers.WriteJson(ctx.Response, new { status = "accepted", id = flowFile.Id, source = Name });
    }

    private async Task HandleHealth(HttpContext ctx)
    {
        await SourceHelpers.WriteJson(ctx.Response, new { status = "healthy", source = Name, running = IsRunning });
    }
}
