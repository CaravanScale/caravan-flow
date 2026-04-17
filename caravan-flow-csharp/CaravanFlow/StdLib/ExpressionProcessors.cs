using CaravanFlow.Core;

namespace CaravanFlow.StdLib;

/// <summary>
/// EvaluateExpression: compute new FlowFile attributes from typed
/// expressions. Config key <c>expressions</c> is a map of target name
/// → expression; each expression evaluates through
/// <see cref="ExpressionEngine"/> (arithmetic, comparisons, booleans,
/// ternary via <c>if(c,a,b)</c>, string ops, math functions).
///
/// Attribute references are bare identifiers — e.g. <c>env</c>,
/// <c>tenant</c>. Dotted paths on nested values aren't meaningful for
/// flat attribute maps. The typed result is stringified for storage
/// (FlowFile attributes are <c>string → string</c>).
///
/// Mirrors caravan-flow-java's JEXL-based EvaluateExpression — both
/// tracks now accept the same expression shape and operator set, so
/// <c>expressions</c> configs are portable between workers.
///
/// Examples:
///   tax:        amount * 0.07
///   label:      upper(region) + "-" + string(round(amount))
///   flag:       amount > 100 &amp;&amp; region == "US"
///   greeting:   "Hello " + name
///   fallback:   coalesce(nickname, name)
/// </summary>
public sealed class EvaluateExpression : IProcessor
{
    private readonly Dictionary<string, CompiledExpression> _compiled;

    public EvaluateExpression(Dictionary<string, string> expressions)
    {
        _compiled = new Dictionary<string, CompiledExpression>(expressions.Count);
        foreach (var (target, expr) in expressions)
        {
            if (string.IsNullOrWhiteSpace(expr))
                throw new ConfigException(target, "expression must not be blank");
            try
            {
                _compiled[target] = ExpressionEngine.Compile(expr);
            }
            catch (FormatException ex)
            {
                throw new ConfigException(target, $"invalid expression '{expr}': {ex.Message}");
            }
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        var result = ff;
        foreach (var (target, compiled) in _compiled)
        {
            EvalValue val;
            try
            {
                val = compiled.Eval(new AttributeValueResolver(result.Attributes.ToDictionary()));
            }
            catch (Exception)
            {
                // Runtime evaluation errors (divide by zero, bad casts)
                // surface as null rather than failing the whole FlowFile.
                // The attribute just doesn't get set; downstream can
                // detect with coalesce() or an isNull(...) check.
                val = EvalValue.Null;
            }
            result = FlowFile.WithAttribute(result, target, val.AsString());
        }
        return SingleResult.Rent(result);
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
