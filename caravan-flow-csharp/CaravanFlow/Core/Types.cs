using System.Buffers;
using System.Runtime.CompilerServices;

namespace CaravanFlow.Core;

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

    /// <summary>Flatten the overlay chain to a plain dictionary.</summary>
    public Dictionary<string, string> ToDictionary()
    {
        var result = new Dictionary<string, string>();
        var overlays = new List<(string Key, string Value)>();
        var current = this;
        while (current is not null)
        {
            if (current._key is not null)
            {
                overlays.Add((current._key, current._value!));
                current = current._parent;
            }
            else
            {
                if (current._base is not null)
                    foreach (var (k, v) in current._base)
                        result[k] = v;
                break;
            }
        }
        // Apply overlays in reverse (most recent overlay wins)
        for (int i = overlays.Count - 1; i >= 0; i--)
            result[overlays[i].Key] = overlays[i].Value;
        return result;
    }
}

// --- Content types (ref-counted for safe ArrayPool return) ---
// Non-atomic: FlowFiles move through pipeline sequentially (one thread per stage).

public abstract class Content
{
    public abstract int Size { get; }
    internal int _refCount = 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AddRef() => _refCount++;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal virtual void Release() => _refCount--;
}

public sealed class Raw : Content
{
    private byte[]? _rented;
    private int _length;
    private bool _pooled;

    public ReadOnlySpan<byte> Data => _rented.AsSpan(0, _length);
    public ReadOnlyMemory<byte> Memory => _rented.AsMemory(0, _length);
    public override int Size => _length;

    // Parameterless constructor for Pool<Raw>
    public Raw()
    {
        _rented = null;
        _length = 0;
        _pooled = false;
    }

    // Legacy constructors (prefer Rent factory methods)
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

    public Raw(byte[] rented, int length, bool pooled = true)
    {
        _rented = rented;
        _length = length;
        _pooled = pooled;
    }

    /// <summary>Pool-friendly factory: rents Raw shell + ArrayPool bytes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Raw Rent(ReadOnlySpan<byte> data)
    {
        var raw = Pool<Raw>.Rent();
        raw._refCount = 1;
        raw._length = data.Length;
        if (data.Length > 0)
        {
            raw._rented = ArrayPool<byte>.Shared.Rent(data.Length);
            data.CopyTo(raw._rented);
            raw._pooled = true;
        }
        else
        {
            raw._rented = Array.Empty<byte>();
            raw._pooled = false;
        }
        return raw;
    }

    /// <summary>Pool-friendly factory: wraps already-rented buffer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Raw Rent(byte[] rented, int length, bool pooled = true)
    {
        var raw = Pool<Raw>.Rent();
        raw._refCount = 1;
        raw._rented = rented;
        raw._length = length;
        raw._pooled = pooled;
        return raw;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal override void Release()
    {
        if (--_refCount == 0)
        {
            if (_pooled && _rented is not null && _rented.Length > 0)
                ArrayPool<byte>.Shared.Return(_rented);
            _rented = null;
            _length = 0;
            _pooled = false;
            Pool<Raw>.Return(this);
        }
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
    public Schema? Schema { get; }
    public List<Record> Records { get; }
    public override int Size => Records.Count;

    public RecordContent(Schema? schema, List<Record> records)
    {
        Schema = schema;
        Records = records;
    }

    public RecordContent(List<Record> records) : this(null, records) { }
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal override void Release()
    {
        if (--_refCount == 0)
            ContentStoreCleanup.Instance?.ReleaseClaim(ClaimId);
    }
}

// --- FlowFile (pooled) ---

public sealed class FlowFile
{
    public long NumericId;
    public AttributeMap Attributes;
    public Content Content;
    public long Timestamp;
    public int HopCount;

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
    public static FlowFile Rent(long id, AttributeMap attributes, Content content, long timestamp, int hopCount = 0)
    {
        var ff = Pool<FlowFile>.Rent();
        ff.NumericId = id;
        ff.Attributes = attributes;
        ff.Content = content;
        ff.Timestamp = timestamp;
        ff.HopCount = hopCount;
        return ff;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(FlowFile ff)
    {
        ff.Content?.Release();
        ff.Content = null!;
        ff.Attributes = null!;
        ff.HopCount = 0;
        Pool<FlowFile>.Return(ff);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile Create(ReadOnlySpan<byte> data, Dictionary<string, string> attributes)
    {
        return Rent(Interlocked.Increment(ref _idCounter), AttributeMap.FromDict(attributes), Raw.Rent(data), Environment.TickCount64);
    }

    private static readonly Dictionary<string, string> EmptyAttrs = new();

    /// <summary>Create FlowFile with empty attributes — avoids Dictionary allocation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile CreateEmpty(ReadOnlySpan<byte> data)
    {
        return Rent(Interlocked.Increment(ref _idCounter), AttributeMap.FromDict(EmptyAttrs), Raw.Rent(data), Environment.TickCount64);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile CreateWithContent(Content content, Dictionary<string, string> attributes)
    {
        return Rent(Interlocked.Increment(ref _idCounter), AttributeMap.FromDict(attributes), content, Environment.TickCount64);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithAttribute(FlowFile ff, string key, string value)
    {
        ff.Content.AddRef(); // shared Content — increment ref count
        return Rent(ff.NumericId, ff.Attributes.With(key, value), ff.Content, ff.Timestamp, ff.HopCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FlowFile WithContent(FlowFile ff, Content content)
    {
        return Rent(ff.NumericId, ff.Attributes, content, ff.Timestamp, ff.HopCount);
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

public sealed class MultipleResult : ProcessorResult
{
    public List<FlowFile> FlowFiles;

    public MultipleResult() { FlowFiles = new List<FlowFile>(); }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MultipleResult Rent()
    {
        var r = Pool<MultipleResult>.Rent();
        r.FlowFiles.Clear();
        return r;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(MultipleResult r)
    {
        r.FlowFiles.Clear();
        Pool<MultipleResult>.Return(r);
    }
}

public sealed class RoutedResult : ProcessorResult
{
    public string Route;
    public FlowFile FlowFile;

    public RoutedResult() { Route = ""; FlowFile = null!; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RoutedResult Rent(string route, FlowFile ff)
    {
        var r = Pool<RoutedResult>.Rent();
        r.Route = route;
        r.FlowFile = ff;
        return r;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(RoutedResult r)
    {
        r.Route = "";
        r.FlowFile = null!;
        Pool<RoutedResult>.Return(r);
    }
}

public sealed class MultiRoutedResult : ProcessorResult
{
    public List<(string Route, FlowFile FlowFile)> Outputs;

    public MultiRoutedResult() { Outputs = new List<(string, FlowFile)>(); }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MultiRoutedResult Rent()
    {
        var r = Pool<MultiRoutedResult>.Rent();
        r.Outputs.Clear();
        return r;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(MultiRoutedResult r)
    {
        r.Outputs.Clear();
        Pool<MultiRoutedResult>.Return(r);
    }
}

public sealed class DroppedResult : ProcessorResult
{
    public static readonly DroppedResult Instance = new();
}

public sealed class FailureResult : ProcessorResult
{
    public string Reason;
    public FlowFile FlowFile;

    public FailureResult() { Reason = ""; FlowFile = null!; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static FailureResult Rent(string reason, FlowFile ff)
    {
        var r = Pool<FailureResult>.Rent();
        r.Reason = reason;
        r.FlowFile = ff;
        return r;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(FailureResult r)
    {
        r.Reason = "";
        r.FlowFile = null!;
        Pool<FailureResult>.Return(r);
    }
}

// --- Processor interface ---

public interface IProcessor
{
    ProcessorResult Process(FlowFile ff);
}

