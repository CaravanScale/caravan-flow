using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using ZincFlow.Core;
using ZincFlow.Fabric;
using ZincFlow.StdLib;
using static ZincFlow.Tests.TestRunner;
using static ZincFlow.Tests.Helpers;

namespace ZincFlow.Tests;

/// <summary>
/// Exercises PutHTTP end-to-end against a real Kestrel mock server.
/// Filling a previously-untested egress path: success, V3 framing roundtrip,
/// X-Flow attribute forwarding, 429 backpressure routing, 500 → FailureResult.
/// </summary>
public static class PutHTTPTests
{
    public static void RunAll()
    {
        TestRawPostSucceeds();
        TestXFlowAttributesForwardedAsHeaders();
        TestV3FormatRoundtripsAttributesAndContent();
        TestServer429RoutesToFailure();
        TestServer500RoutesToFailure();
        TestUnreachableEndpointFails();
    }

    /// <summary>
    /// Minimal Kestrel server that records every request it receives.
    /// One handler swappable per test via the <see cref="Handler"/> field.
    /// </summary>
    private sealed class MockReceiver : IDisposable
    {
        public string BaseUrl { get; }
        public int RequestCount { get; private set; }
        public Func<HttpContext, Task>? Handler { get; set; }
        public List<(string ContentType, byte[] Body, IHeaderDictionary Headers)> Received { get; } = new();
        private readonly WebApplication _app;

        public MockReceiver(int port)
        {
            BaseUrl = $"http://127.0.0.1:{port}";
            var builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls(BaseUrl);
            _app = builder.Build();
            _app.Map("/", (RequestDelegate)HandleAny);
            _app.Map("/{*path}", (RequestDelegate)HandleAny);
            _ = _app.RunAsync();
            WaitForHttpReady(BaseUrl);
            // The readiness probe itself counts as a recorded request — discard it
            // so per-test assertions start from a clean slate.
            Received.Clear();
            RequestCount = 0;
        }

        private async Task HandleAny(HttpContext ctx)
        {
            RequestCount++;
            using var ms = new MemoryStream();
            await ctx.Request.Body.CopyToAsync(ms);
            var body = ms.ToArray();
            var headers = new HeaderDictionary();
            foreach (var h in ctx.Request.Headers) headers[h.Key] = h.Value;
            Received.Add((ctx.Request.ContentType ?? "", body, headers));

            if (Handler is not null) { await Handler(ctx); return; }
            ctx.Response.StatusCode = 200;
            await ctx.Response.WriteAsync("ok");
        }

        public void Dispose() => _app.StopAsync().Wait(TimeSpan.FromSeconds(2));
    }

    private static IContentStore Store() => new MemoryContentStore();

    static void TestRawPostSucceeds()
    {
        Console.WriteLine("--- PutHTTP: raw POST returns SingleResult with delivery attrs ---");
        using var mock = new MockReceiver(FreePort());
        var put = new PutHTTP(mock.BaseUrl, "raw", Store());
        var ff = FlowFile.Create("hello"u8.ToArray(), new() { ["filename"] = "x.txt" });

        var result = put.Process(ff);
        AssertTrue("returns SingleResult", result is SingleResult);
        var delivered = ((SingleResult)result).FlowFile;
        AssertTrue("delivery.status set",
            delivered.Attributes.TryGetValue("delivery.status", out var s) && s == "200");
        AssertTrue("delivery.endpoint set",
            delivered.Attributes.TryGetValue("delivery.endpoint", out var e) && e == mock.BaseUrl);
        AssertIntEqual("server received 1 request", mock.RequestCount, 1);
        AssertEqual("body delivered as-is", Encoding.UTF8.GetString(mock.Received[0].Body), "hello");
    }

    static void TestXFlowAttributesForwardedAsHeaders()
    {
        Console.WriteLine("--- PutHTTP: raw mode forwards attributes via X-Flow-* headers ---");
        using var mock = new MockReceiver(FreePort());
        var put = new PutHTTP(mock.BaseUrl, "raw", Store());
        var ff = FlowFile.Create("payload"u8.ToArray(), new()
        {
            ["filename"] = "data.bin",
            ["batch.id"] = "abc-123",
            ["http.content.type"] = "application/json"  // should NOT be forwarded as X-Flow-* (filtered)
        });

        put.Process(ff);

        var headers = mock.Received[0].Headers;
        AssertTrue("X-Flow-filename forwarded", headers.ContainsKey("X-Flow-filename"));
        AssertTrue("X-Flow-batch.id forwarded", headers.ContainsKey("X-Flow-batch.id"));
        AssertFalse("http.* attributes filtered out", headers.ContainsKey("X-Flow-http.content.type"));
        AssertEqual("Content-Type came from http.content.type",
            mock.Received[0].ContentType.Split(';')[0], "application/json");
    }

    static void TestV3FormatRoundtripsAttributesAndContent()
    {
        Console.WriteLine("--- PutHTTP: format=v3 sends V3-framed body that round-trips ---");
        using var mock = new MockReceiver(FreePort());
        var put = new PutHTTP(mock.BaseUrl, "v3", Store());
        var ff = FlowFile.Create("v3 payload"u8.ToArray(), new()
        {
            ["filename"] = "v3.bin",
            ["env"] = "prod",
            ["batch.id"] = "xyz-789"
        });

        put.Process(ff);

        AssertEqual("server saw octet-stream", mock.Received[0].ContentType.Split(';')[0], "application/octet-stream");
        var body = mock.Received[0].Body;
        AssertTrue("body starts with NiFiFF3 magic",
            body.Length >= 7 && Encoding.UTF8.GetString(body, 0, 7) == "NiFiFF3");

        // Decode the body — every original attribute should survive.
        var unpacked = FlowFileV3.UnpackAll(body);
        AssertIntEqual("one frame round-tripped", unpacked.Count, 1);
        AssertTrue("env attr preserved",
            unpacked[0].Attributes.TryGetValue("env", out var env) && env == "prod");
        AssertTrue("batch.id preserved",
            unpacked[0].Attributes.TryGetValue("batch.id", out var b) && b == "xyz-789");
    }

    static void TestServer429RoutesToFailure()
    {
        Console.WriteLine("--- PutHTTP: HTTP 429 → FailureResult tagged backpressure ---");
        using var mock = new MockReceiver(FreePort());
        mock.Handler = ctx => { ctx.Response.StatusCode = 429; return Task.CompletedTask; };

        var put = new PutHTTP(mock.BaseUrl, "raw", Store());
        var ff = FlowFile.Create("x"u8.ToArray(), new());
        var result = put.Process(ff);
        AssertTrue("429 → FailureResult", result is FailureResult);
        var failure = (FailureResult)result;
        AssertTrue("error mentions backpressure", failure.Reason.Contains("backpressure"));
    }

    static void TestServer500RoutesToFailure()
    {
        Console.WriteLine("--- PutHTTP: HTTP 500 → FailureResult ---");
        using var mock = new MockReceiver(FreePort());
        mock.Handler = ctx => { ctx.Response.StatusCode = 500; return Task.CompletedTask; };

        var put = new PutHTTP(mock.BaseUrl, "raw", Store());
        var result = put.Process(FlowFile.Create("x"u8.ToArray(), new()));
        AssertTrue("500 → FailureResult", result is FailureResult);
        var failure = (FailureResult)result;
        AssertTrue("error mentions 500", failure.Reason.Contains("500"));
    }

    static void TestUnreachableEndpointFails()
    {
        Console.WriteLine("--- PutHTTP: unreachable endpoint → FailureResult, no crash ---");
        var deadPort = FreePort();  // grab a port and don't bind to it — guaranteed connection-refused
        var put = new PutHTTP($"http://127.0.0.1:{deadPort}/", "raw", Store());
        var result = put.Process(FlowFile.Create("x"u8.ToArray(), new()));
        AssertTrue("unreachable → FailureResult", result is FailureResult);
    }
}
