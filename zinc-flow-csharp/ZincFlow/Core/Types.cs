using System.Buffers;
using System.Runtime.CompilerServices;

namespace ZincFlow.Core;

// --- Thread-local object pool: zero contention, zero CAS ---

internal static class Pool<T> where T : class, new()
{
    private const int MaxPerThread = 256;

    [ThreadStatic]
    private static T[]? t_items;

    [ThreadStatic]
    private static int t_count;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T Rent()
    {
        var items = t_items;
        if (items is not null && t_count > 0)
        {
            var obj = items[--t_count];
            items[t_count] = null!;
            return obj;
        }
        return new T();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(T obj)
    {
        var items = t_items;
        if (items is null)
        {
            items = new T[MaxPerThread];
            t_items = items;
        }
        if (t_count < MaxPerThread)
        {
            items[t_count++] = obj;
        }
    }
}

// --- Component lifecycle ---

public enum ComponentState { Disabled, Enabled, Draining }

// --- Attribute map: immutable overlay chain (avoids Dictionary copy on WithAttribute) ---

public sealed class AttributeMap
{
    // Base layer: the original dictionary (shared, never copied)
    private readonly Dictionary<string, string>? _base;
    // Overlay chain: single key/value override on top of parent
    private readonly AttributeMap? _parent;
    private readonly string? _key;
    private readonly string? _value;
    private readonly int _count;

    // Construct from a dictionary (initial FlowFile creation)
    public AttributeMap(Dictionary<string, string> baseAttrs)
    {
        _base = baseAttrs;
        _parent = null;
        _key = null;
        _value = null;
        _count = baseAttrs.Count;
    }

    // Construct an overlay (WithAttribute — zero dict copy)
    private AttributeMap(AttributeMap parent, string key, string value)
    {
        _base = null;
        _parent = parent;
        _key = key;
        _value = value;
        // Count: parent count + 1 if new key, else same
        _count = parent.ContainsKey(key) ? parent._count : parent._count + 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AttributeMap With(string key, string value) => new(this, key, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ContainsKey(string key)
    {
        var current = this;
        while (current is not null)
        {
            if (current._key is not null)
            {
                if (string.Equals(current._key, key, StringComparison.Ordinal))
                    return true;
                current = current._parent;
            }
            else
            {
                return current._base!.ContainsKey(key);
            }
        }
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(string key, out string value)
    {
        var current = this;
        while (current is not null)
        {
            if (current._key is not null)
            {
                if (string.Equals(current._key, key, StringComparison.Ordinal))
                {
                    value = current._value!;
                    return true;
                }
                current = current._parent;
            }
            else
            {
                return current._base!.TryGetValue(key, out value!);
            }
        }
        value = default!;
        return false;
    }

    public int Count => _count;
}

// --- Content types ---

public abstract class Content
{
    public abstract int Size { get; }
}

public sealed class Raw : Content
{
    private byte[]? _rented;
    private readonly int _length;
    private readonly bool _pooled;

    public ReadOnlySpan<byte> Data => _rented.AsSpan(0, _length);
    public ReadOnlyMemory<byte> Memory => _rented.AsMemory(0, _length);
    public override int Size => _length;

    public Raw(ReadOnlySpan<byte> data)
    {
        _length = data.Length;
        if (_length > 0)
        {
            _rented = ArrayPool<byte>.Shared.Rent(_length);
            data.CopyTo(_rented);
            _pooled = true;
        }
        else
        {
            _rented = Array.Empty<byte>();
            _pooled = false;
        }
    }

    // Fast path: wrap an already-rented buffer (caller transfers ownership)
    public Raw(byte[] rented, int length, bool pooled = true)
    {
        _rented = rented;
        _length = length;
        _pooled = pooled;
    }

    public void Return()
    {
        if (_pooled && _rented is not null && _rented.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_rented);
            _rented = null;
        }
    }
}

public sealed class RecordContent : Content
{
    public Dictionary<string, string> Schema { get; }
    public List<Dictionary<string, object?>> Records { get; }
    public override int Size => Records.Count;

    public RecordContent(Dictionary<string, string> schema, List<Dictionary<string, object?>> records)
    {
        Schema = schema;
        Records = records;
    }
}

public sealed class ClaimContent : Content
{
    public string ClaimId { get; }
    public override int Size { get; }

    public ClaimContent(string claimId, int size)
    {
        ClaimId = claimId;
        Size = size;
    }
}

// --- FlowFile (pooled) ---

public sealed class FlowFile
{
    public string Id;
    public AttributeMap Attributes;
    public Content Content;
    public long Timestamp;

    private static long _idCounter;

    public FlowFile()
    {
        Id = "";
        Attributes = null!;
        Content = null!;
        Timestamp = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile Rent(string id, AttributeMap attributes, Content content, long timestamp)
    {
        var ff = Pool<FlowFile>.Rent();
        ff.Id = id;
        ff.Attributes = attributes;
        ff.Content = content;
        ff.Timestamp = timestamp;
        return ff;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(FlowFile ff)
    {
        ff.Content = null!;
        ff.Attributes = null!;
        Pool<FlowFile>.Return(ff);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile Create(ReadOnlySpan<byte> data, Dictionary<string, string> attributes)
    {
        var id = string.Create(null, stackalloc char[24], $"ff-{Interlocked.Increment(ref _idCounter)}");
        return Rent(id, new AttributeMap(attributes), new Raw(data), Environment.TickCount64);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithAttribute(FlowFile ff, string key, string value)
    {
        // Zero-copy: overlay chain instead of Dictionary copy
        return Rent(ff.Id, ff.Attributes.With(key, value), ff.Content, ff.Timestamp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithContent(FlowFile ff, Content content)
    {
        return Rent(ff.Id, ff.Attributes, content, ff.Timestamp);
    }
}

// --- Processor results (pooled SingleResult) ---

public abstract class ProcessorResult { }

public sealed class SingleResult : ProcessorResult
{
    public FlowFile FlowFile;

    public SingleResult() { FlowFile = null!; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SingleResult Rent(FlowFile ff)
    {
        var r = Pool<SingleResult>.Rent();
        r.FlowFile = ff;
        return r;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(SingleResult r)
    {
        r.FlowFile = null!;
        Pool<SingleResult>.Return(r);
    }
}

public sealed class DroppedResult : ProcessorResult
{
    public static readonly DroppedResult Instance = new();
}

public sealed class FailureResult : ProcessorResult
{
    public string Reason { get; }
    public FlowFile FlowFile { get; }
    public FailureResult(string reason, FlowFile ff) { Reason = reason; FlowFile = ff; }
}

// --- Processor interface ---

public interface IProcessor
{
    ProcessorResult Process(FlowFile ff);
}

// --- Queue entry (pooled) ---

public sealed class QueueEntry
{
    public string Id;
    public FlowFile FlowFile;
    public long ClaimedAt;
    public int AttemptCount;
    public string SourceProcessor;

    public QueueEntry()
    {
        Id = "";
        FlowFile = null!;
        ClaimedAt = 0;
        AttemptCount = 0;
        SourceProcessor = "";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static QueueEntry Rent(string id, FlowFile flowFile, long claimedAt, int attemptCount, string sourceProcessor)
    {
        var e = Pool<QueueEntry>.Rent();
        e.Id = id;
        e.FlowFile = flowFile;
        e.ClaimedAt = claimedAt;
        e.AttemptCount = attemptCount;
        e.SourceProcessor = sourceProcessor;
        return e;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(QueueEntry e)
    {
        e.FlowFile = null!;
        Pool<QueueEntry>.Return(e);
    }
}

// --- DLQ entry ---

public sealed class DLQEntry
{
    public string Id { get; }
    public FlowFile FlowFile { get; }
    public string SourceProcessor { get; }
    public string SourceQueue { get; }
    public int AttemptCount { get; }
    public string LastError { get; }
    public double ArrivedAt { get; }

    public DLQEntry(string id, FlowFile ff, string sourceProc, string sourceQueue, int attempts, string error, double arrivedAt)
    {
        Id = id;
        FlowFile = ff;
        SourceProcessor = sourceProc;
        SourceQueue = sourceQueue;
        AttemptCount = attempts;
        LastError = error;
        ArrivedAt = arrivedAt;
    }
}
