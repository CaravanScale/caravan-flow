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
    internal Dictionary<string, string>? _base;
    // Overlay chain: single key/value override on top of parent
    internal AttributeMap? _parent;
    internal string? _key;
    internal string? _value;
    internal int _count;

    public AttributeMap()
    {
        _base = null;
        _parent = null;
        _key = null;
        _value = null;
        _count = 0;
    }

    // Construct from a dictionary (initial FlowFile creation)
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static AttributeMap FromDict(Dictionary<string, string> baseAttrs)
    {
        var map = Pool<AttributeMap>.Rent();
        map._base = baseAttrs;
        map._parent = null;
        map._key = null;
        map._value = null;
        map._count = baseAttrs.Count;
        return map;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(AttributeMap map)
    {
        map._base = null;
        map._parent = null;
        map._key = null;
        map._value = null;
        Pool<AttributeMap>.Return(map);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AttributeMap With(string key, string value)
    {
        var overlay = Pool<AttributeMap>.Rent();
        overlay._base = null;
        overlay._parent = this;
        overlay._key = key;
        overlay._value = value;
        overlay._count = this.ContainsKey(key) ? this._count : this._count + 1;
        return overlay;
    }

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
    public long NumericId;
    public AttributeMap Attributes;
    public Content Content;
    public long Timestamp;

    private static long _idCounter;

    public FlowFile()
    {
        NumericId = 0;
        Attributes = null!;
        Content = null!;
        Timestamp = 0;
    }

    // String ID for display/debugging only (not on hot path)
    public string Id => $"ff-{NumericId}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile Rent(long id, AttributeMap attributes, Content content, long timestamp)
    {
        var ff = Pool<FlowFile>.Rent();
        ff.NumericId = id;
        ff.Attributes = attributes;
        ff.Content = content;
        ff.Timestamp = timestamp;
        return ff;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(FlowFile ff)
    {
        // Content and AttributeMap are NOT returned here because they may be
        // shared with sibling FlowFiles:
        //   - WithAttribute creates overlay on same parent chain
        //   - WithContent shares the same Content reference
        // The FlowFile shell is the only thing we can safely pool.
        // AttributeMap overlay nodes and Raw byte arrays are reclaimed by GC.
        // ArrayPool.Rent for Raw avoids LOH on initial alloc (the main win).
        ff.Content = null!;
        ff.Attributes = null!;
        Pool<FlowFile>.Return(ff);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile Create(ReadOnlySpan<byte> data, Dictionary<string, string> attributes)
    {
        return Rent(Interlocked.Increment(ref _idCounter), AttributeMap.FromDict(attributes), new Raw(data), Environment.TickCount64);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithAttribute(FlowFile ff, string key, string value)
    {
        return Rent(ff.NumericId, ff.Attributes.With(key, value), ff.Content, ff.Timestamp);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithContent(FlowFile ff, Content content)
    {
        return Rent(ff.NumericId, ff.Attributes, content, ff.Timestamp);
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
    public long Id;
    public FlowFile FlowFile;
    public long ClaimedAt;
    public int AttemptCount;
    public string SourceProcessor;

    public QueueEntry()
    {
        Id = 0;
        FlowFile = null!;
        ClaimedAt = 0;
        AttemptCount = 0;
        SourceProcessor = "";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static QueueEntry Rent(long id, FlowFile flowFile, long claimedAt, int attemptCount, string sourceProcessor)
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
