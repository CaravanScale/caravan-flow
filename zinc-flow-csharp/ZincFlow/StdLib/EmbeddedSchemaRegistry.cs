using System.Collections.Concurrent;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// In-process schema registry. The sole backend in airgapped deployments —
/// no external HTTP service, schemas live in this process.
///
/// Three population paths:
///   - <see cref="LoadFromConfig"/> at startup / hot-reload (declarative)
///   - <see cref="RegisterAsync"/> via REST (runtime, ad-hoc)
///   - Auto-capture from <c>ConvertOCFToRecord</c> (writer schema in incoming
///     <c>.avro</c> files registered automatically; identical-schema dedup
///     means re-reading a known file is a no-op)
///
/// Storage is in-memory only. Restart loses runtime registrations;
/// config-loaded ones come back. Disk persistence is a future follow-up.
///
/// API surface mirrors the methods <see cref="ConvertOCFToRecord"/> uses
/// (<see cref="GetByIdAsync"/>, <see cref="GetSubjectVersionAsync"/>,
/// <see cref="RegisterAsync"/>) plus admin helpers consumed by the REST handler.
/// </summary>
public sealed class EmbeddedSchemaRegistry
{
    internal readonly record struct VersionEntry(int Id, int Version, Schema Schema, string CanonicalJson);

    private readonly ConcurrentDictionary<int, Schema> _byId = new();
    private readonly ConcurrentDictionary<string, List<VersionEntry>> _subjects = new();
    private readonly ConcurrentDictionary<string, object> _subjectLocks = new();
    private int _nextId; // bumped via Interlocked.Increment, so first id is 1

    // --- Read API (Task-returning to match the call shape ConvertOCFToRecord uses) ---

    public Task<Schema> GetByIdAsync(int id, CancellationToken ct = default)
    {
        if (_byId.TryGetValue(id, out var schema)) return Task.FromResult(schema);
        throw new InvalidOperationException($"schema id {id} not found");
    }

    public Task<(int Id, Schema Schema)> GetSubjectVersionAsync(string subject, string version, CancellationToken ct = default)
    {
        if (!_subjects.TryGetValue(subject, out var versions) || versions.Count == 0)
            throw new InvalidOperationException($"subject '{subject}' not found");

        VersionEntry entry;
        if (version == "latest")
        {
            lock (LockFor(subject)) entry = versions[^1];
        }
        else if (int.TryParse(version, out var v))
        {
            lock (LockFor(subject))
            {
                var found = versions.FirstOrDefault(x => x.Version == v);
                if (found.Id == 0)
                    throw new InvalidOperationException($"subject '{subject}' version {v} not found");
                entry = found;
            }
        }
        else
        {
            throw new InvalidOperationException($"version must be an integer or 'latest', got '{version}'");
        }
        return Task.FromResult((entry.Id, entry.Schema));
    }

    /// <summary>
    /// Register a schema under a subject. Confluent-style identical-schema
    /// detection: re-registering an exact match (after JSON canonicalization)
    /// returns the existing id without bumping the version. Otherwise the
    /// schema gets a new id and is appended as the next version.
    /// </summary>
    public Task<int> RegisterAsync(string subject, Schema schema, CancellationToken ct = default)
    {
        var canonical = AvroSchemaJson.Emit(schema);
        var versions = _subjects.GetOrAdd(subject, _ => new List<VersionEntry>());
        lock (LockFor(subject))
        {
            foreach (var existing in versions)
                if (existing.CanonicalJson == canonical) return Task.FromResult(existing.Id);

            var id = Interlocked.Increment(ref _nextId);
            var nextVersion = versions.Count == 0 ? 1 : versions[^1].Version + 1;
            var entry = new VersionEntry(id, nextVersion, schema, canonical);
            versions.Add(entry);
            _byId[id] = schema;
            return Task.FromResult(id);
        }
    }

    // --- Admin API (used by REST handler; not on the read path) ---

    public List<string> ListSubjects() => new(_subjects.Keys);

    public List<int> ListVersions(string subject)
        => _subjects.TryGetValue(subject, out var v)
            ? v.Select(e => e.Version).ToList()
            : new List<int>();

    public bool DeleteSubject(string subject)
    {
        if (!_subjects.TryRemove(subject, out var versions)) return false;
        foreach (var e in versions) _byId.TryRemove(e.Id, out _);
        _subjectLocks.TryRemove(subject, out _);
        return true;
    }

    public bool DeleteVersion(string subject, int version)
    {
        if (!_subjects.TryGetValue(subject, out var versions)) return false;
        lock (LockFor(subject))
        {
            var idx = versions.FindIndex(e => e.Version == version);
            if (idx < 0) return false;
            _byId.TryRemove(versions[idx].Id, out _);
            versions.RemoveAt(idx);
            if (versions.Count == 0)
            {
                _subjects.TryRemove(subject, out _);
                _subjectLocks.TryRemove(subject, out _);
            }
            return true;
        }
    }

    /// <summary>
    /// Bulk-load schemas from a parsed YAML <c>schemas:</c> section. Each
    /// subject is either <c>{inline: "&lt;schema json&gt;"}</c> or
    /// <c>{file: "path/to/schema.avsc"}</c> (path resolved relative to
    /// <paramref name="configDir"/>). Returns the count successfully loaded.
    ///
    /// Uses <see cref="RegisterAsync"/>, so re-loading an unchanged config
    /// is a no-op (identical-schema dedup). Loading a changed schema for an
    /// existing subject creates a new version.
    ///
    /// Errors (parse failures, missing files) log a warning to stderr and
    /// skip the offending subject — boot does not abort.
    /// </summary>
    public int LoadFromConfig(Dictionary<string, object?>? schemasSection, string? configDir)
    {
        if (schemasSection is null) return 0;
        int loaded = 0;
        foreach (var (subject, defObj) in schemasSection)
        {
            try
            {
                if (defObj is not Dictionary<string, object?> def)
                {
                    Console.Error.WriteLine($"[schema-registry] schemas.{subject}: expected mapping with 'inline' or 'file', got {defObj?.GetType().Name ?? "null"}");
                    continue;
                }

                string? schemaJson = null;
                if (def.TryGetValue("inline", out var inlineObj) && inlineObj is string inline)
                {
                    schemaJson = inline;
                }
                else if (def.TryGetValue("file", out var fileObj) && fileObj is string file)
                {
                    var path = Path.IsPathRooted(file) || configDir is null
                        ? file
                        : Path.Combine(configDir, file);
                    if (!File.Exists(path))
                    {
                        Console.Error.WriteLine($"[schema-registry] schemas.{subject}: file not found: {path}");
                        continue;
                    }
                    schemaJson = File.ReadAllText(path);
                }
                else
                {
                    Console.Error.WriteLine($"[schema-registry] schemas.{subject}: must have 'inline' or 'file' key");
                    continue;
                }

                var schema = AvroSchemaJson.Parse(schemaJson);
                RegisterAsync(subject, schema).GetAwaiter().GetResult();
                loaded++;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[schema-registry] schemas.{subject}: {ex.GetType().Name}: {ex.Message}");
            }
        }
        return loaded;
    }

    // --- Internal helpers exposed to the REST handler ---

    internal VersionEntry? GetEntryByIdInternal(int id)
    {
        foreach (var versions in _subjects.Values)
            foreach (var e in versions)
                if (e.Id == id) return e;
        return null;
    }

    internal bool TryGetEntry(string subject, int version, out VersionEntry entry)
    {
        entry = default;
        if (!_subjects.TryGetValue(subject, out var versions)) return false;
        lock (LockFor(subject))
        {
            var found = versions.FirstOrDefault(e => e.Version == version);
            if (found.Id == 0) return false;
            entry = found;
            return true;
        }
    }

    internal bool TryGetLatest(string subject, out VersionEntry entry)
    {
        entry = default;
        if (!_subjects.TryGetValue(subject, out var versions) || versions.Count == 0) return false;
        lock (LockFor(subject)) entry = versions[^1];
        return true;
    }

    private object LockFor(string subject)
        => _subjectLocks.GetOrAdd(subject, _ => new object());
}
