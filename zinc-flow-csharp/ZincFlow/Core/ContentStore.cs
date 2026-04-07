using System.Buffers;
using System.Runtime.CompilerServices;

namespace ZincFlow.Core;

// --- ContentStore interface ---

public interface IContentStore
{
    string Store(byte[] data);
    byte[] Retrieve(string claimId);
    void Delete(string claimId);
    bool Exists(string claimId);
}

// --- FileContentStore: filesystem-backed with sharded directories ---

public sealed class FileContentStore : IContentStore
{
    private readonly string _baseDir;
    private long _claimCounter;

    public FileContentStore(string baseDir)
    {
        _baseDir = baseDir;
        Directory.CreateDirectory(baseDir);
    }

    public string Store(byte[] data)
    {
        var claimId = GenerateClaimId();
        var path = ClaimPath(claimId);
        File.WriteAllBytes(path, data);
        return claimId;
    }

    public byte[] Retrieve(string claimId)
    {
        var path = ClaimPath(claimId);
        if (!File.Exists(path)) return [];
        return File.ReadAllBytes(path);
    }

    public void Delete(string claimId)
    {
        var path = ClaimPath(claimId);
        if (File.Exists(path)) File.Delete(path);
    }

    public bool Exists(string claimId)
    {
        return File.Exists(ClaimPath(claimId));
    }

    private string ClaimPath(string claimId)
    {
        var prefix = claimId[..Math.Min(2, claimId.Length)];
        var dir = Path.Combine(_baseDir, prefix);
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, claimId);
    }

    private string GenerateClaimId()
    {
        var counter = Interlocked.Increment(ref _claimCounter);
        return $"claim-{Environment.TickCount64}-{counter}";
    }
}

// --- MemoryContentStore: in-memory for testing ---

public sealed class MemoryContentStore : IContentStore
{
    private readonly Dictionary<string, byte[]> _data = new();
    private readonly object _lock = new();
    private long _counter;

    public string Store(byte[] data)
    {
        var id = $"mem-claim-{Interlocked.Increment(ref _counter)}";
        lock (_lock) { _data[id] = data; }
        return id;
    }

    public byte[] Retrieve(string claimId)
    {
        lock (_lock) { return _data.GetValueOrDefault(claimId) ?? []; }
    }

    public void Delete(string claimId)
    {
        lock (_lock) { _data.Remove(claimId); }
    }

    public bool Exists(string claimId)
    {
        lock (_lock) { return _data.ContainsKey(claimId); }
    }
}

// --- Content helpers ---

public static class ContentHelpers
{
    public const int ClaimThreshold = 256 * 1024;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Content MaybeOffload(IContentStore store, byte[] data)
    {
        if (data.Length > ClaimThreshold)
        {
            var claimId = store.Store(data);
            return new ClaimContent(claimId, data.Length);
        }
        return new Raw(data);
    }

    public static (byte[] Data, string Error) Resolve(IContentStore store, Content content)
    {
        if (content is Raw raw)
            return (raw.Data.ToArray(), "");
        if (content is ClaimContent claim)
            return (store.Retrieve(claim.ClaimId), "");
        if (content is RecordContent)
            return ([], "cannot resolve Records to raw bytes — use a RecordWriter");
        return ([], "unknown content type");
    }
}
