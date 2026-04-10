namespace ZincFlow.Core;

/// <summary>
/// Source connector: produces FlowFiles from an external system.
/// Lifecycle: Start → Running → Stop → Stopped.
///
/// Two patterns:
///   Event-driven (ListenHTTP, GetNats) — implement IConnectorSource directly,
///     use library's native threading, call ingest() when data arrives.
///   Polling (GetFile, GetFTP) — extend PollingSource, implement Poll(),
///     framework handles scheduling, lifecycle, backpressure.
/// </summary>
public interface IConnectorSource
{
    string Name { get; }
    string SourceType { get; }
    bool IsRunning { get; }
    void Start(Func<FlowFile, bool> ingest, CancellationToken ct);
    void Stop();
}

/// <summary>
/// Base class for polling-based source connectors.
/// Subclasses implement Poll() to produce FlowFiles. The base class handles
/// scheduling (PeriodicTimer), lifecycle, backpressure, and error isolation.
/// Concurrency comes from the Fabric's execution gate, not from the source.
/// </summary>
public abstract class PollingSource : IConnectorSource
{
    public string Name { get; }
    public abstract string SourceType { get; }
    public bool IsRunning { get; private set; }

    private readonly int _pollIntervalMs;
    private Func<FlowFile, bool> _ingest = null!;
    private PeriodicTimer? _timer;
    private CancellationTokenSource? _cts;

    protected PollingSource(string name, int pollIntervalMs)
    {
        Name = name;
        _pollIntervalMs = pollIntervalMs > 0 ? pollIntervalMs : 1000;
    }

    /// <summary>
    /// Produce FlowFiles for this poll cycle. Called on the timer thread.
    /// Return an empty list if nothing to process.
    /// </summary>
    protected abstract List<FlowFile> Poll(CancellationToken ct);

    /// <summary>
    /// Called after a FlowFile is successfully ingested (pipeline accepted it).
    /// Override to perform post-ingest work (e.g., move file to .processed).
    /// </summary>
    protected virtual void OnIngested(FlowFile ff) { }

    /// <summary>
    /// Called when a FlowFile is rejected due to backpressure.
    /// Default: returns FlowFile to pool. Override for custom behavior.
    /// </summary>
    protected virtual void OnRejected(FlowFile ff) { FlowFile.Return(ff); }

    public void Start(Func<FlowFile, bool> ingest, CancellationToken ct)
    {
        _ingest = ingest;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_pollIntervalMs));
        IsRunning = true;
        _ = Task.Run(() => RunLoop(_cts.Token), _cts.Token);
    }

    public void Stop()
    {
        IsRunning = false;
        _cts?.Cancel();
        _timer?.Dispose();
    }

    private async Task RunLoop(CancellationToken ct)
    {
        try
        {
            while (await _timer!.WaitForNextTickAsync(ct))
            {
                try
                {
                    var batch = Poll(ct);
                    foreach (var ff in batch)
                    {
                        if (ct.IsCancellationRequested) break;
                        if (_ingest(ff))
                            OnIngested(ff);
                        else
                            OnRejected(ff);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[{SourceType}] {Name} poll error: {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException) { }
        finally { IsRunning = false; }
    }
}

/// <summary>
/// Delivery result from PutHTTP / PutFile / etc.
/// </summary>
public sealed class DeliveryResult
{
    public bool Success { get; }
    public int StatusCode { get; }
    public string Error { get; }

    public DeliveryResult(bool success, int statusCode = 0, string error = "")
    {
        Success = success;
        StatusCode = statusCode;
        Error = error;
    }

    public static readonly DeliveryResult Ok = new(true);
}
