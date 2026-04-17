using CaravanFlow.Core;
using CaravanFlow.Fabric;

namespace CaravanFlow.StdLib;

// --- Shared helpers ---

internal static class SourceHelpers
{
    internal static Task WriteJson(HttpResponse response, object value)
    {
        response.ContentType = "application/json";
        return response.WriteAsync(CaravanFlow.Core.CaravanJson.SerializeToString(value));
    }
}

/// <summary>
/// GetFile: polls a directory and ingests new files as FlowFiles.
/// Files are moved to a .processed subdirectory after ingestion.
/// Extends PollingSource — framework handles scheduling and lifecycle.
/// </summary>
public sealed class GetFile : PollingSource
{
    public override string SourceType => "GetFile";

    // V3 magic header bytes: "NiFiFF3"
    private static readonly byte[] V3Magic = "NiFiFF3"u8.ToArray();

    private readonly string _inputDir;
    private readonly string _processedDir;
    private readonly string _pattern;
    private readonly IContentStore _store;
    private readonly bool _unpackV3;

    public GetFile(string name, string inputDir, string pattern, int pollIntervalMs, IContentStore store,
        bool unpackV3 = true)
        : base(name, pollIntervalMs)
    {
        _inputDir = inputDir;
        _processedDir = Path.Combine(inputDir, ".processed");
        _pattern = string.IsNullOrEmpty(pattern) ? "*" : pattern;
        _store = store;
        _unpackV3 = unpackV3;
        Directory.CreateDirectory(_inputDir);
        Directory.CreateDirectory(_processedDir);
    }

    protected override List<FlowFile> Poll(CancellationToken ct)
    {
        var results = new List<FlowFile>();
        try
        {
            foreach (var filePath in Directory.GetFiles(_inputDir, _pattern))
            {
                if (ct.IsCancellationRequested) break;
                try
                {
                    var fileName = Path.GetFileName(filePath);
                    var data = File.ReadAllBytes(filePath);

                    // Sniff V3 magic. A V3-framed file may contain N FlowFiles concatenated;
                    // each gets its original attributes restored. Source attributes (filename
                    // etc.) are layered on top so downstream still knows where it came from.
                    if (_unpackV3 && IsV3(data))
                    {
                        var unpacked = FlowFileV3.UnpackAll(data);
                        for (int i = 0; i < unpacked.Count; i++)
                        {
                            var ff = unpacked[i];
                            ff = FlowFile.WithAttribute(ff, "filename", fileName);
                            ff = FlowFile.WithAttribute(ff, "path", filePath);
                            ff = FlowFile.WithAttribute(ff, "source", Name);
                            ff = FlowFile.WithAttribute(ff, "v3.frame.index", i.ToString());
                            ff = FlowFile.WithAttribute(ff, "v3.frame.count", unpacked.Count.ToString());
                            results.Add(ff);
                        }
                        continue;
                    }

                    var content = ContentHelpers.MaybeOffload(_store, data);
                    results.Add(FlowFile.CreateWithContent(content, new Dictionary<string, string>
                    {
                        ["filename"] = fileName,
                        ["path"] = filePath,
                        ["size"] = data.Length.ToString(),
                        ["source"] = Name
                    }));
                }
                catch (IOException) { }
            }
        }
        catch (DirectoryNotFoundException) { Directory.CreateDirectory(_inputDir); }
        return results;
    }

    private static bool IsV3(byte[] data)
        => data.Length >= V3Magic.Length && data.AsSpan(0, V3Magic.Length).SequenceEqual(V3Magic);

    protected override void OnIngested(FlowFile ff)
    {
        if (!ff.Attributes.TryGetValue("path", out var filePath)) return;
        var fileName = Path.GetFileName(filePath);
        var dest = Path.Combine(_processedDir, fileName);
        try
        {
            if (File.Exists(dest)) File.Delete(dest);
            File.Move(filePath, dest);
        }
        catch (IOException) { }
    }
}

/// <summary>
/// ListenHTTP: starts its own HTTP server on a dedicated port.
/// Accepts FlowFile ingestion (raw body or NiFi V3 binary).
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
                    options.SerializerOptions.TypeInfoResolverChain.Insert(0, CaravanFlow.Core.CaravanJsonContext.Default);
                });

                _app = builder.Build();
                _app.Map(_path, (RequestDelegate)HandleRequest);
                _app.Map("/health", (RequestDelegate)HandleHealth);

                Console.WriteLine($"[ListenHTTP] {Name} listening on :{_port}{_path}");
                await _app.RunAsync(_cts.Token);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { Console.Error.WriteLine($"[ListenHTTP] {Name} error: {ex.Message}"); }
            finally { IsRunning = false; }
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
            await SourceHelpers.WriteJson(ctx.Response, new Dictionary<string, object?> { ["error"] = "method not allowed" });
            return;
        }

        using var ms = new MemoryStream();
        await ctx.Request.Body.CopyToAsync(ms);
        var body = ms.ToArray();

        // Accept either the official NiFi MIME type (application/flowfile-v3) or
        // the bare octet-stream (NiFi's older Site-to-Site default) for V3 framing.
        var contentType = ctx.Request.ContentType ?? "";
        if (contentType == "application/flowfile-v3" || contentType == "application/octet-stream")
        {
            var flowfiles = FlowFileV3.UnpackAll(body);
            int accepted = 0;
            foreach (var ff in flowfiles)
            {
                if (_ingest(ff))
                    accepted++;
                else
                    FlowFile.Return(ff); // return rejected FlowFiles to pool
            }
            await SourceHelpers.WriteJson(ctx.Response, new Dictionary<string, object?> { ["status"] = "accepted", ["count"] = accepted, ["source"] = Name });
            return;
        }

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
            FlowFile.Return(flowFile); // return to pool on backpressure
            ctx.Response.StatusCode = 503;
            await SourceHelpers.WriteJson(ctx.Response, new Dictionary<string, object?> { ["error"] = "backpressure", ["source"] = Name });
            return;
        }

        await SourceHelpers.WriteJson(ctx.Response, new Dictionary<string, object?> { ["status"] = "accepted", ["id"] = flowFile.Id, ["source"] = Name });
    }

    private async Task HandleHealth(HttpContext ctx)
    {
        await SourceHelpers.WriteJson(ctx.Response, new Dictionary<string, object?> { ["status"] = "healthy", ["source"] = Name, ["running"] = IsRunning });
    }
}

/// <summary>
/// GenerateFlowFile: produces FlowFiles on a schedule for testing, load testing, and heartbeats.
/// Extends PollingSource — framework handles scheduling and lifecycle.
///
/// Config:
///   content         — FlowFile content string (default: empty)
///   content_type    — set as http.content.type attribute
///   attributes      — semicolon-separated key:value pairs
///   batch_size      — FlowFiles per poll cycle (default: 1)
///   poll_interval_ms — polling interval (default: 1000)
/// </summary>
public sealed class GenerateFlowFile : PollingSource
{
    public override string SourceType => "GenerateFlowFile";

    private readonly byte[] _content;
    private readonly Dictionary<string, string> _baseAttrs;
    private readonly int _batchSize;
    private static long _counter;

    public GenerateFlowFile(string name, int pollIntervalMs, string content, string contentType, string attributes, int batchSize)
        : base(name, pollIntervalMs)
    {
        _content = System.Text.Encoding.UTF8.GetBytes(content ?? "");
        _batchSize = batchSize > 0 ? batchSize : 1;

        _baseAttrs = new Dictionary<string, string> { ["source"] = name };
        if (!string.IsNullOrEmpty(contentType))
            _baseAttrs["http.content.type"] = contentType;

        if (!string.IsNullOrEmpty(attributes))
        {
            foreach (var pair in attributes.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = pair.Split(':', 2);
                if (kv.Length == 2)
                    _baseAttrs[kv[0]] = kv[1];
            }
        }
    }

    protected override List<FlowFile> Poll(CancellationToken ct)
    {
        var batch = new List<FlowFile>(_batchSize);
        for (int i = 0; i < _batchSize; i++)
        {
            var attrs = new Dictionary<string, string>(_baseAttrs)
            {
                ["generate.index"] = Interlocked.Increment(ref _counter).ToString()
            };
            batch.Add(FlowFile.Create(_content, attrs));
        }
        return batch;
    }
}
