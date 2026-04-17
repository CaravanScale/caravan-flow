using System.Text;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;

namespace CaravanFlow.Core;

/// <summary>
/// Loads a base config plus optional local / secrets overlays and
/// deep-merges them into a single effective config. Tracks which
/// layer supplied each dot-path so <c>GET /api/overlays</c> can
/// report provenance.
///
/// Layer order (later wins): base ← local ← secrets.
///
/// Overlay paths come from (in order):
///   1. Explicit argument (used by tests and the admin API's
///      <c>PUT /api/overlays/secrets</c> write-through path).
///   2. Environment variable (<c>CARAVANFLOW_CONFIG_LOCAL</c>,
///      <c>CARAVANFLOW_SECRETS_PATH</c>).
///   3. Sibling of the base config (<c>config.local.yaml</c>,
///      <c>secrets.yaml</c>).
///
/// A missing file at any layer is not an error — the layer simply
/// contributes an empty map.
///
/// Mirrors caravan-flow-java's <c>ConfigOverlay</c> shape so the
/// HTTP surface (<c>GET /api/overlays</c>, <c>PUT /api/overlays/secrets</c>)
/// returns the same JSON on both tracks.
/// </summary>
public static class Overlay
{
    public const string DefaultLocalName = "config.local.yaml";
    public const string DefaultSecretsName = "secrets.yaml";
    public const string EnvLocal = "CARAVANFLOW_CONFIG_LOCAL";
    public const string EnvSecrets = "CARAVANFLOW_SECRETS_PATH";

    /// <summary>One layer in the overlay stack.</summary>
    public sealed class Layer
    {
        public string Role { get; }
        public string? Path { get; }
        public bool Present { get; }
        public Dictionary<string, object?> Content { get; }

        public Layer(string role, string? path, bool present, Dictionary<string, object?>? content)
        {
            Role = role;
            Path = path;
            Present = present;
            Content = content is null ? new() : DeepCopy(content);
        }
    }

    /// <summary>Result of a <see cref="Load"/> call.</summary>
    public sealed class Resolved
    {
        public string? BasePath { get; }
        public List<Layer> Layers { get; }
        public Dictionary<string, object?> Effective { get; }
        public Dictionary<string, string> Provenance { get; }

        public Resolved(string? basePath, List<Layer> layers, Dictionary<string, object?> effective, Dictionary<string, string> provenance)
        {
            BasePath = basePath;
            Layers = layers;
            Effective = effective;
            Provenance = provenance;
        }
    }

    /// <summary>
    /// Load base + sibling overlays using env-var / default-name rules.
    /// </summary>
    public static Resolved Load(string? basePath)
        => Load(basePath, ResolveLocalPath(basePath), ResolveSecretsPath(basePath));

    /// <summary>
    /// Load with explicit overlay paths. Missing files contribute empty
    /// maps; the returned Resolved.Layers entries track Present=false
    /// so the admin API can show path + missing state.
    /// </summary>
    public static Resolved Load(string? basePath, string? localPath, string? secretsPath)
    {
        var baseLayer    = ReadLayer("base",    basePath);
        var localLayer   = ReadLayer("local",   localPath);
        var secretsLayer = ReadLayer("secrets", secretsPath);

        var effective = new Dictionary<string, object?>();
        var provenance = new Dictionary<string, string>();
        Merge(effective, provenance, baseLayer.Content, baseLayer.Role, "");
        Merge(effective, provenance, localLayer.Content, localLayer.Role, "");
        Merge(effective, provenance, secretsLayer.Content, secretsLayer.Role, "");
        return new Resolved(basePath, new List<Layer> { baseLayer, localLayer, secretsLayer }, effective, provenance);
    }

    public static string? ResolveLocalPath(string? basePath)
    {
        var env = Environment.GetEnvironmentVariable(EnvLocal);
        if (!string.IsNullOrEmpty(env)) return env;
        if (string.IsNullOrEmpty(basePath)) return DefaultLocalName;
        var dir = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(basePath));
        return dir is null ? DefaultLocalName : System.IO.Path.Combine(dir, DefaultLocalName);
    }

    public static string? ResolveSecretsPath(string? basePath)
    {
        var env = Environment.GetEnvironmentVariable(EnvSecrets);
        if (!string.IsNullOrEmpty(env)) return env;
        if (string.IsNullOrEmpty(basePath)) return DefaultSecretsName;
        var dir = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(basePath));
        return dir is null ? DefaultSecretsName : System.IO.Path.Combine(dir, DefaultSecretsName);
    }

    private static Layer ReadLayer(string role, string? path)
    {
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
            return new Layer(role, path, present: false, content: null);
        var yaml = File.ReadAllText(path);
        if (string.IsNullOrWhiteSpace(yaml))
            return new Layer(role, path, present: true, content: null);
        var parsed = YamlParser.Parse(yaml);
        return new Layer(role, path, present: true, content: parsed);
    }

    /// Recursive deep-merge: src onto dst with dot-path provenance.
    private static void Merge(Dictionary<string, object?> dst,
                              Dictionary<string, string> provenance,
                              Dictionary<string, object?> src,
                              string layerRole,
                              string parentPath)
    {
        if (src is null || src.Count == 0) return;
        foreach (var (key, value) in src)
        {
            var path = parentPath.Length == 0 ? key : parentPath + "." + key;
            var existing = dst.GetValueOrDefault(key);
            if (existing is Dictionary<string, object?> existingMap
                && value is Dictionary<string, object?> incomingMap)
            {
                var mergedChild = new Dictionary<string, object?>(existingMap);
                Merge(mergedChild, provenance, incomingMap, layerRole, path);
                dst[key] = mergedChild;
            }
            else
            {
                dst[key] = DeepCopyValue(value);
                provenance[path] = layerRole;
            }
        }
    }

    private static Dictionary<string, object?> DeepCopy(Dictionary<string, object?> src)
    {
        var output = new Dictionary<string, object?>(src.Count);
        foreach (var (k, v) in src) output[k] = DeepCopyValue(v);
        return output;
    }

    private static object? DeepCopyValue(object? v)
    {
        if (v is Dictionary<string, object?> m) return DeepCopy(m);
        if (v is List<object?> l)
        {
            var copy = new List<object?>(l.Count);
            foreach (var o in l) copy.Add(DeepCopyValue(o));
            return copy;
        }
        return v;
    }

    /// <summary>
    /// Write a YAML document representing the secrets overlay to disk.
    /// Used by <c>PUT /api/overlays/secrets</c>. Caller passes the
    /// sanitized secrets map — this method does not re-layer.
    ///
    /// Uses YamlDotNet's low-level IEmitter (token-based, AOT-safe;
    /// no reflection or dynamic code) so the native binary keeps its
    /// no-reflection guarantee.
    /// </summary>
    public static void WriteSecrets(string path, Dictionary<string, object?>? secrets)
    {
        if (string.IsNullOrEmpty(path)) throw new ArgumentException("secrets path must not be null or empty");
        var parent = System.IO.Path.GetDirectoryName(System.IO.Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(parent)) Directory.CreateDirectory(parent);

        var sb = new StringBuilder();
        using (var sw = new StringWriter(sb))
        {
            var emitter = new Emitter(sw);
            emitter.Emit(new StreamStart());
            emitter.Emit(new DocumentStart());
            EmitValue(emitter, secrets ?? new Dictionary<string, object?>());
            emitter.Emit(new DocumentEnd(isImplicit: true));
            emitter.Emit(new StreamEnd());
        }
        File.WriteAllText(path, sb.ToString());
    }

    private static void EmitValue(IEmitter emitter, object? v)
    {
        switch (v)
        {
            case null:
                emitter.Emit(new Scalar(null, null, "null", ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                break;
            case Dictionary<string, object?> map:
                emitter.Emit(new MappingStart(null, null, isImplicit: true, MappingStyle.Block));
                foreach (var (k, inner) in map)
                {
                    emitter.Emit(new Scalar(null, null, k, ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                    EmitValue(emitter, inner);
                }
                emitter.Emit(new MappingEnd());
                break;
            case List<object?> list:
                emitter.Emit(new SequenceStart(null, null, isImplicit: true, SequenceStyle.Block));
                foreach (var inner in list) EmitValue(emitter, inner);
                emitter.Emit(new SequenceEnd());
                break;
            case bool b:
                emitter.Emit(new Scalar(null, null, b ? "true" : "false", ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                break;
            default:
                // Strings get double-quoted — secrets contain special chars often
                // enough that unquoted strings would be a foot-gun. Non-string
                // scalars (numbers) go out plain.
                if (v is string s)
                    emitter.Emit(new Scalar(null, null, s, ScalarStyle.DoubleQuoted, isPlainImplicit: false, isQuotedImplicit: true));
                else
                    emitter.Emit(new Scalar(null, null, v.ToString() ?? "", ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                break;
        }
    }
}
