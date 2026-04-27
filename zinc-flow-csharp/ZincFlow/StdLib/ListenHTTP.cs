using System.Net;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// ListenHTTP: binds an HttpListener on a configured port and ingests
/// each POST body as a FlowFile. Event-driven source — implements
/// IConnectorSource directly (not PollingSource).
///
/// Existence of this source is the answer to "how do I POST a file to
/// the flow?" — in visual programming, ingress belongs on the canvas
/// as a source node. Operators pick the port and path; the worker's
/// management API stays on its own port (default 9091).
///
/// Request headers prefixed with <c>X-Flow-</c> become FlowFile
/// attributes (header <c>X-Flow-filename</c> → attribute
/// <c>filename</c>). Returns 202 on accept, 503 on backpressure.
/// </summary>
public sealed class ListenHTTP : IConnectorSource
{
    public string Name { get; }
    public string SourceType => "ListenHTTP";
    public bool IsRunning { get; private set; }

    private readonly int _port;
    private readonly string _path;
    private readonly long _maxBodyBytes;
    private HttpListener? _listener;
    private CancellationTokenSource? _cts;
    private Func<FlowFile, bool>? _ingest;

    public ListenHTTP(string name, int port, string path, long maxBodyBytes = 16 * 1024 * 1024)
    {
        Name = name;
        _port = port;
        var normalized = string.IsNullOrEmpty(path) ? "/" : (path.StartsWith("/") ? path : "/" + path);
        // HttpListener prefixes require a trailing slash.
        _path = normalized.EndsWith("/") ? normalized : normalized + "/";
        _maxBodyBytes = maxBodyBytes > 0 ? maxBodyBytes : 16 * 1024 * 1024;
    }

    public void Start(Func<FlowFile, bool> ingest, CancellationToken ct)
    {
        _ingest = ingest;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{_port}{_path}");
        try
        {
            _listener.Start();
        }
        catch (HttpListenerException ex)
        {
            Console.Error.WriteLine($"[ListenHTTP] {Name}: bind failed on :{_port}{_path} — {ex.Message}");
            IsRunning = false;
            return;
        }
        IsRunning = true;
        _ = Task.Run(() => RunLoop(_cts.Token), _cts.Token);
    }

    public void Stop()
    {
        IsRunning = false;
        _cts?.Cancel();
        try { _listener?.Stop(); } catch { }
        try { _listener?.Close(); } catch { }
        _listener = null;
    }

    private async Task RunLoop(CancellationToken ct)
    {
        var listener = _listener!;
        while (!ct.IsCancellationRequested && listener.IsListening)
        {
            HttpListenerContext ctx;
            try { ctx = await listener.GetContextAsync(); }
            catch (HttpListenerException) { break; }
            catch (ObjectDisposedException) { break; }

            _ = Task.Run(() => HandleRequest(ctx, ct), ct);
        }
    }

    private async Task HandleRequest(HttpListenerContext ctx, CancellationToken ct)
    {
        var req = ctx.Request;
        var res = ctx.Response;
        try
        {
            if (req.HttpMethod != "POST" && req.HttpMethod != "PUT")
            {
                res.StatusCode = 405;
                res.AddHeader("Allow", "POST, PUT");
                return;
            }

            // Reject over-limit uploads before allocating a buffer for them.
            // Content-Length isn't authoritative for chunked encoding so we
            // also cap the copy below with a running-total check.
            if (req.ContentLength64 > _maxBodyBytes)
            {
                res.StatusCode = 413;
                return;
            }

            byte[] bytes;
            using (var ms = new MemoryStream())
            {
                var buf = new byte[8192];
                long total = 0;
                while (true)
                {
                    int n = await req.InputStream.ReadAsync(buf.AsMemory(0, buf.Length), ct);
                    if (n <= 0) break;
                    total += n;
                    if (total > _maxBodyBytes)
                    {
                        res.StatusCode = 413;
                        return;
                    }
                    ms.Write(buf, 0, n);
                }
                bytes = ms.ToArray();
            }

            var attrs = new Dictionary<string, string>();
            if (!string.IsNullOrEmpty(req.ContentType)) attrs["content.type"] = req.ContentType;
            foreach (string? key in req.Headers.AllKeys)
            {
                if (key is null) continue;
                var lower = key.ToLowerInvariant();
                if (lower.StartsWith("x-flow-"))
                    attrs[lower.Substring("x-flow-".Length)] = req.Headers[key] ?? "";
            }

            var ff = FlowFile.Create(bytes, attrs);
            bool ok = _ingest?.Invoke(ff) ?? false;
            res.StatusCode = ok ? 202 : 503;
            await res.OutputStream.WriteAsync(
                System.Text.Encoding.UTF8.GetBytes($"ff-{ff.NumericId}"), ct);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[ListenHTTP] {Name}: request error — {ex.Message}");
            try { res.StatusCode = 500; } catch { }
        }
        finally
        {
            try { res.Close(); } catch { }
        }
    }
}
