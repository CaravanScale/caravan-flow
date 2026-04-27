using System.Text;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;

namespace ZincFlow.Core;

/// <summary>
/// YAML emitter for nested <c>Dictionary&lt;string, object?&gt;</c>
/// configs. Used by <c>POST /api/flow/save</c> to serialize the
/// runtime graph back into <c>config.yaml</c>.
///
/// Uses YamlDotNet's low-level <c>IEmitter</c> — token-based, AOT-safe;
/// no reflection, no dynamic code — so the worker's native-AOT build
/// stays free of reflection dependencies.
///
/// Output shape is the canonical separated layout enforced by the
/// parity cohort: <c>flow.entryPoints</c> list, <c>flow.processors</c>
/// map, <c>flow.connections</c> map (all as siblings, not inlined per
/// processor). Top-level keys emit in the Dictionary's insertion order
/// — callers should build the dict with that order so diffs stay
/// stable between saves.
/// </summary>
public static class YamlEmitter
{
    public static byte[] Emit(Dictionary<string, object?> root)
    {
        var sb = new StringBuilder();
        using (var sw = new StringWriter(sb))
        {
            var emitter = new Emitter(sw);
            emitter.Emit(new StreamStart());
            emitter.Emit(new DocumentStart());
            EmitValue(emitter, root);
            emitter.Emit(new DocumentEnd(isImplicit: true));
            emitter.Emit(new StreamEnd());
        }
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private static void EmitValue(IEmitter emitter, object? value)
    {
        switch (value)
        {
            case null:
                emitter.Emit(new Scalar(null, null, "null", ScalarStyle.Plain,
                    isPlainImplicit: true, isQuotedImplicit: false));
                break;
            case Dictionary<string, object?> map:
                emitter.Emit(new MappingStart(null, null, isImplicit: true, MappingStyle.Block));
                foreach (var (k, v) in map)
                {
                    emitter.Emit(new Scalar(null, null, k, ScalarStyle.Plain,
                        isPlainImplicit: true, isQuotedImplicit: false));
                    EmitValue(emitter, v);
                }
                emitter.Emit(new MappingEnd());
                break;
            case IDictionary<string, object?> idict:
                emitter.Emit(new MappingStart(null, null, isImplicit: true, MappingStyle.Block));
                foreach (var (k, v) in idict)
                {
                    emitter.Emit(new Scalar(null, null, k, ScalarStyle.Plain,
                        isPlainImplicit: true, isQuotedImplicit: false));
                    EmitValue(emitter, v);
                }
                emitter.Emit(new MappingEnd());
                break;
            case IDictionary<string, string> sdict:
                emitter.Emit(new MappingStart(null, null, isImplicit: true, MappingStyle.Block));
                foreach (var (k, v) in sdict)
                {
                    emitter.Emit(new Scalar(null, null, k, ScalarStyle.Plain,
                        isPlainImplicit: true, isQuotedImplicit: false));
                    EmitScalarString(emitter, v);
                }
                emitter.Emit(new MappingEnd());
                break;
            case IEnumerable<string> strList:
                emitter.Emit(new SequenceStart(null, null, isImplicit: true, SequenceStyle.Flow));
                foreach (var s in strList) EmitScalarString(emitter, s);
                emitter.Emit(new SequenceEnd());
                break;
            case System.Collections.IEnumerable enumerable when value is not string:
                emitter.Emit(new SequenceStart(null, null, isImplicit: true, SequenceStyle.Block));
                foreach (var item in enumerable) EmitValue(emitter, item);
                emitter.Emit(new SequenceEnd());
                break;
            case bool b:
                emitter.Emit(new Scalar(null, null, b ? "true" : "false",
                    ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                break;
            case string s:
                EmitScalarString(emitter, s);
                break;
            default:
                emitter.Emit(new Scalar(null, null, value.ToString() ?? "",
                    ScalarStyle.Plain, isPlainImplicit: true, isQuotedImplicit: false));
                break;
        }
    }

    /// <summary>
    /// String scalars are emitted plain when safe, double-quoted when
    /// they contain characters that would confuse a plain-parse
    /// (colons, leading indicators, whitespace-only, reserved words
    /// like "null"/"true"/"false"/numbers). Mirrors SnakeYAML's
    /// default-safer behavior on the Java side so round-trips produce
    /// byte-identical output after YamlParser → YamlEmitter.
    /// </summary>
    private static void EmitScalarString(IEmitter emitter, string? value)
    {
        if (value is null)
        {
            emitter.Emit(new Scalar(null, null, "null", ScalarStyle.Plain,
                isPlainImplicit: true, isQuotedImplicit: false));
            return;
        }
        if (NeedsQuoting(value))
        {
            emitter.Emit(new Scalar(null, null, value, ScalarStyle.DoubleQuoted,
                isPlainImplicit: false, isQuotedImplicit: true));
        }
        else
        {
            emitter.Emit(new Scalar(null, null, value, ScalarStyle.Plain,
                isPlainImplicit: true, isQuotedImplicit: false));
        }
    }

    private static bool NeedsQuoting(string v)
    {
        if (v.Length == 0) return true;
        // Reserved tokens that would re-parse as non-string
        if (v == "null" || v == "true" || v == "false" || v == "yes" || v == "no") return true;
        if (double.TryParse(v, System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture, out _)) return true;
        // Chars that break plain-style parsing
        foreach (var c in v)
        {
            if (c == ':' || c == '#' || c == '\n' || c == '\r' || c == '\t' ||
                c == '"' || c == '\'' || c == '{' || c == '}' || c == '[' || c == ']' ||
                c == ',' || c == '&' || c == '*' || c == '!' || c == '|' || c == '>' ||
                c == '%' || c == '@' || c == '`') return true;
        }
        // Leading/trailing whitespace or leading indicator chars
        if (char.IsWhiteSpace(v[0]) || char.IsWhiteSpace(v[^1])) return true;
        if (v[0] == '-' || v[0] == '?' || v[0] == ':' || v[0] == '#') return true;
        return false;
    }
}
