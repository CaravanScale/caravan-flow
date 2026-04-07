namespace ZincFlow.Core;

/// <summary>
/// Source connector: produces FlowFiles from an external system.
/// Lifecycle: Start → Running → Stop → Stopped.
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
