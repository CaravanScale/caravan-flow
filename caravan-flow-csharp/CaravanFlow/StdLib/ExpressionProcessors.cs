using System.Text.RegularExpressions;
using CaravanFlow.Core;

namespace CaravanFlow.StdLib;

/// <summary>
/// EvaluateExpression: compute new FlowFile attributes from expressions on existing attributes.
/// Config: expressions as key=value pairs where value is an expression.
///
/// Expression syntax:
///   ${attr}                    — attribute reference
///   ${attr:toUpper()}          — toUpper
///   ${attr:toLower()}          — toLower
///   ${attr:substring(s,e)}    — substring (start, end)
///   ${attr:replace(old,new)}  — string replace
///   ${attr:length()}          — string length
///   ${attr:trim()}            — trim whitespace
///   ${attr:append(suffix)}    — append text
///   ${attr:prepend(prefix)}   — prepend text
///   literal text              — passed through as-is
///   ${attr}/${other}          — concatenation via mixed literal/expression
/// </summary>
public sealed class EvaluateExpression : IProcessor
{
    private readonly Dictionary<string, string> _expressions;
    private static readonly Regex ExprPattern = new(@"\$\{([^}]+)\}", RegexOptions.Compiled);

    public EvaluateExpression(Dictionary<string, string> expressions)
    {
        _expressions = expressions;
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var result = ff;
        foreach (var (target, expr) in _expressions)
        {
            var value = Evaluate(expr, result.Attributes);
            result = FlowFile.WithAttribute(result, target, value);
        }
        return SingleResult.Rent(result);
    }

    public static string Evaluate(string expression, AttributeMap attrs)
    {
        return ExprPattern.Replace(expression, match =>
        {
            var inner = match.Groups[1].Value;
            return EvaluateReference(inner, attrs);
        });
    }

    private static string EvaluateReference(string expr, AttributeMap attrs)
    {
        // Parse: attrName or attrName:func(args)
        var colonIdx = expr.IndexOf(':');
        string attrName;
        string? funcChain;

        if (colonIdx < 0)
        {
            attrName = expr;
            funcChain = null;
        }
        else
        {
            attrName = expr[..colonIdx];
            funcChain = expr[(colonIdx + 1)..];
        }

        if (!attrs.TryGetValue(attrName, out var value))
            value = "";

        if (funcChain is null)
            return value;

        // Apply chained functions: func1():func2(arg)
        foreach (var call in ParseFuncChain(funcChain))
        {
            value = ApplyFunction(value, call.Name, call.Args);
        }
        return value;
    }

    private static string ApplyFunction(string value, string funcName, string[] args)
    {
        return funcName switch
        {
            "toUpper" => value.ToUpperInvariant(),
            "toLower" => value.ToLowerInvariant(),
            "trim" => value.Trim(),
            "length" => value.Length.ToString(),
            "substring" => args.Length >= 2
                && int.TryParse(args[0], out var s)
                && int.TryParse(args[1], out var e)
                ? value.Substring(Math.Min(s, value.Length), Math.Min(e - s, value.Length - Math.Min(s, value.Length)))
                : value,
            "replace" => args.Length >= 2 ? value.Replace(args[0], args[1]) : value,
            "append" => args.Length >= 1 ? value + args[0] : value,
            "prepend" => args.Length >= 1 ? args[0] + value : value,
            "contains" => args.Length >= 1 ? value.Contains(args[0], StringComparison.Ordinal).ToString().ToLowerInvariant() : "false",
            "startsWith" => args.Length >= 1 ? value.StartsWith(args[0], StringComparison.Ordinal).ToString().ToLowerInvariant() : "false",
            "endsWith" => args.Length >= 1 ? value.EndsWith(args[0], StringComparison.Ordinal).ToString().ToLowerInvariant() : "false",
            "defaultIfEmpty" => string.IsNullOrEmpty(value) && args.Length >= 1 ? args[0] : value,
            _ => value
        };
    }

    private static List<(string Name, string[] Args)> ParseFuncChain(string chain)
    {
        var calls = new List<(string, string[])>();
        int i = 0;
        while (i < chain.Length)
        {
            // Find function name
            int parenOpen = chain.IndexOf('(', i);
            if (parenOpen < 0) break;
            var name = chain[i..parenOpen];

            // Find matching close paren
            int parenClose = chain.IndexOf(')', parenOpen);
            if (parenClose < 0) break;

            var argsStr = chain[(parenOpen + 1)..parenClose];
            var args = string.IsNullOrEmpty(argsStr)
                ? Array.Empty<string>()
                : SplitArgs(argsStr);

            calls.Add((name, args));

            // Skip past ')' and optional ':'
            i = parenClose + 1;
            if (i < chain.Length && chain[i] == ':')
                i++;
        }
        return calls;
    }

    private static string[] SplitArgs(string argsStr)
    {
        // Simple comma split, respecting single quotes
        var args = new List<string>();
        var sb = new System.Text.StringBuilder();
        bool inQuote = false;
        foreach (char c in argsStr)
        {
            if (c == '\'' && !inQuote) { inQuote = true; continue; }
            if (c == '\'' && inQuote) { inQuote = false; continue; }
            if (c == ',' && !inQuote) { args.Add(sb.ToString()); sb.Clear(); continue; }
            sb.Append(c);
        }
        args.Add(sb.ToString());
        return args.ToArray();
    }
}

/// <summary>
/// TransformRecord: field-level operations on RecordContent.
/// Config: operations as semicolon-separated directives.
///
/// Directives:
///   rename:oldName:newName       — rename a field
///   remove:fieldName             — remove a field
///   add:fieldName:value          — add a string-typed field with a literal value
///   copy:source:target           — copy field value to new field (preserves type)
///   toUpper:fieldName            — uppercase a string field
///   toLower:fieldName            — lowercase a string field
///   default:fieldName:value      — set field to string value if null/missing
///   compute:targetField:expression — evaluate expression against record fields,
///                                    store the typed result. Each compute step
///                                    sees the field state from prior steps.
///
/// Output schema preserves the type of unmodified fields and infers types from
/// the first record for new/computed fields. Schema is no longer flattened to
/// String — Long stays Long, Double stays Double, etc.
/// </summary>
public sealed class TransformRecord : IProcessor
{
    private readonly List<(string Op, string Arg1, string Arg2)> _operations;
    private readonly Dictionary<string, CompiledExpression> _compiled = new();

    public TransformRecord(string operations)
    {
        _operations = ParseOperations(operations);
        // Pre-compile compute expressions; keep failed parses out of the cache so
        // the runtime sees the field as missing rather than crashing the pipeline.
        foreach (var (op, _, expr) in _operations)
        {
            if (op == "compute" && !string.IsNullOrEmpty(expr) && !_compiled.ContainsKey(expr))
            {
                try { _compiled[expr] = ExpressionEngine.Compile(expr); }
                catch (FormatException) { /* skip; runtime treats it as a no-op */ }
            }
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        // Apply operations to a per-record dict copy.
        var transformedDicts = new List<Dictionary<string, object?>>(rc.Records.Count);
        foreach (var record in rc.Records)
        {
            var dict = record.ToDictionary();
            foreach (var (op, arg1, arg2) in _operations)
                ApplyOp(dict, op, arg1, arg2);
            transformedDicts.Add(dict);
        }

        // Build the output schema. For each final field, prefer the original FieldType
        // when the field name pre-existed and we haven't overwritten its kind; otherwise
        // infer from the first record's value.
        var origByName = rc.Schema.Fields.ToDictionary(f => f.Name, f => f);
        var firstDict = transformedDicts[0];
        var newFields = new List<Field>(firstDict.Count);
        foreach (var (name, value) in firstDict)
        {
            var inferred = InferFieldType(value);
            FieldType chosen;
            if (origByName.TryGetValue(name, out var origField) && IsCompatible(origField.FieldType, inferred))
                chosen = origField.FieldType;
            else
                chosen = inferred == FieldType.Null ? FieldType.String : inferred;
            newFields.Add(new Field(name, chosen));
        }
        var newSchema = new Schema(rc.Schema.Name, newFields);

        var transformed = new List<GenericRecord>(transformedDicts.Count);
        foreach (var dict in transformedDicts)
        {
            var rec = new GenericRecord(newSchema);
            foreach (var (k, v) in dict) rec.SetField(k, v);
            transformed.Add(rec);
        }

        return SingleResult.Rent(FlowFile.WithContent(ff, new RecordContent(newSchema, transformed)));
    }

    private void ApplyOp(Dictionary<string, object?> dict, string op, string arg1, string arg2)
    {
        switch (op)
        {
            case "rename":
                if (dict.Remove(arg1, out var renameVal))
                    dict[arg2] = renameVal;
                break;
            case "remove":
                dict.Remove(arg1);
                break;
            case "add":
                dict[arg1] = arg2;
                break;
            case "copy":
                if (dict.TryGetValue(arg1, out var copyVal))
                    dict[arg2] = copyVal;
                break;
            case "toUpper":
                if (dict.TryGetValue(arg1, out var upperVal) && upperVal is string us)
                    dict[arg1] = us.ToUpperInvariant();
                break;
            case "toLower":
                if (dict.TryGetValue(arg1, out var lowerVal) && lowerVal is string ls)
                    dict[arg1] = ls.ToLowerInvariant();
                break;
            case "default":
                if (!dict.ContainsKey(arg1) || dict[arg1] is null)
                    dict[arg1] = arg2;
                break;
            case "compute":
                if (_compiled.TryGetValue(arg2, out var expr))
                {
                    var resolver = new DictValueResolver(dict);
                    dict[arg1] = expr.Eval(resolver).ToObject();
                }
                break;
        }
    }

    private static FieldType InferFieldType(object? v) => v switch
    {
        null => FieldType.Null,
        bool => FieldType.Boolean,
        int => FieldType.Int,
        long => FieldType.Long,
        short => FieldType.Int,
        byte => FieldType.Int,
        float => FieldType.Float,
        double => FieldType.Double,
        string => FieldType.String,
        byte[] => FieldType.Bytes,
        _ => FieldType.String
    };

    // Original type wins when both are numeric (don't downgrade Long→Int) or
    // both are strings, or both null. Otherwise the inferred (post-transform) type wins.
    private static bool IsCompatible(FieldType orig, FieldType inferred)
    {
        if (orig == inferred) return true;
        if (inferred == FieldType.Null) return true;
        bool origNumeric = orig is FieldType.Int or FieldType.Long or FieldType.Float or FieldType.Double;
        bool infNumeric = inferred is FieldType.Int or FieldType.Long or FieldType.Float or FieldType.Double;
        return origNumeric && infNumeric;
    }

    // Known op vocabulary. Unknown op names (typo like "uppper") silently
    // fell through in ApplyOp before — now we reject at construction time.
    // Keep in sync with the switch in ApplyOp.
    private static readonly HashSet<string> _knownOps = new(StringComparer.Ordinal)
    {
        "rename", "remove", "add", "copy", "toUpper", "toLower", "default", "compute"
    };

    private static List<(string, string, string)> ParseOperations(string ops)
    {
        var result = new List<(string, string, string)>();
        if (string.IsNullOrWhiteSpace(ops)) return result;
        foreach (var directive in ops.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = directive.Split(':', 3);
            if (parts.Length < 2)
                throw new ConfigException(
                    $"TransformRecord: malformed directive '{directive}' — expected 'op:arg1' or 'op:arg1:arg2'");
            var op = parts[0];
            if (!_knownOps.Contains(op))
                throw new ConfigException(
                    $"TransformRecord: unknown op '{op}' in directive '{directive}' — valid: {string.Join(", ", _knownOps)}");
            result.Add((op, parts[1], parts.Length > 2 ? parts[2] : ""));
        }
        return result;
    }
}
