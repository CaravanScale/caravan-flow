using ZincFlow.Core;

namespace ZincFlow.StdLib;

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
/// Mirrors zinc-flow-java's JEXL-based EvaluateExpression — both
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
        // infer from the first record's value. If the input had no schema, all types
        // are inferred from values.
        var origByName = rc.Schema is not null
            ? rc.Schema.Fields.ToDictionary(f => f.Name, f => f)
            : new Dictionary<string, Field>();
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
        var newSchema = new Schema(rc.Schema?.Name ?? "record", newFields);

        var transformed = new List<Record>(transformedDicts.Count);
        foreach (var dict in transformedDicts)
        {
            var rec = new Record(newSchema);
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

/// <summary>
/// UpdateRecord: set or derive record fields via expressions. The record-level
/// counterpart to <see cref="EvaluateExpression"/> (which updates FlowFile
/// attributes). Config key <c>updates</c> is a semicolon-separated list of
/// <c>fieldName = expression</c> pairs.
///
/// Each expression evaluates through <see cref="ExpressionEngine"/> with a
/// <see cref="DictValueResolver"/> over a mutable working copy of the record,
/// so assignments are visible to later expressions in the same directive list
/// (left-to-right). The original record is not mutated — a new Record is
/// emitted per input.
///
/// The output schema preserves original field types for unchanged names, infers
/// from values for new/changed fields, and keeps the schema name. If the input
/// is schemaless the output is too (names inferred from record keys).
///
/// Examples:
///   tax   = amount * 0.07
///   total = amount + tax
///   label = upper(region) + "-" + string(round(total))
/// </summary>
public sealed class UpdateRecord : IProcessor
{
    private readonly List<(string Field, string ExprStr, CompiledExpression Compiled)> _updates;

    public UpdateRecord(string updates)
    {
        _updates = new List<(string, string, CompiledExpression)>();
        if (string.IsNullOrWhiteSpace(updates)) return;
        foreach (var pair in updates.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = pair.IndexOf('=');
            if (eq <= 0)
                throw new ConfigException(
                    $"UpdateRecord: malformed pair '{pair}' — expected 'field = expression'");
            var field = pair[..eq].Trim();
            var exprStr = pair[(eq + 1)..].Trim();
            if (field.Length == 0)
                throw new ConfigException($"UpdateRecord: empty field name in '{pair}'");
            if (exprStr.Length == 0)
                throw new ConfigException($"UpdateRecord: empty expression for field '{field}'");
            try
            {
                _updates.Add((field, exprStr, ExpressionEngine.Compile(exprStr)));
            }
            catch (FormatException ex)
            {
                throw new ConfigException(
                    $"UpdateRecord: field '{field}' has invalid expression '{exprStr}': {ex.Message}");
            }
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        var updatedDicts = new List<Dictionary<string, object?>>(rc.Records.Count);
        foreach (var record in rc.Records)
        {
            var dict = record.ToDictionary();
            foreach (var (field, _, compiled) in _updates)
            {
                object? val;
                try { val = compiled.Eval(new DictValueResolver(dict)).ToObject(); }
                // Runtime errors (divide-by-zero, bad cast) surface as null for
                // that field; the rest of the record still flows. Catches the
                // same class of errors EvaluateExpression swallows for attrs.
                catch { val = null; }
                dict[field] = val;
            }
            updatedDicts.Add(dict);
        }

        // Build output schema: original field type wins for unchanged names;
        // inferred type wins for new/changed fields.
        var origByName = rc.Schema is not null
            ? rc.Schema.Fields.ToDictionary(f => f.Name, f => f)
            : new Dictionary<string, Field>();
        var updatedFieldNames = new HashSet<string>(_updates.Select(u => u.Field), StringComparer.Ordinal);
        var firstDict = updatedDicts[0];
        var newFields = new List<Field>(firstDict.Count);
        foreach (var (name, value) in firstDict)
        {
            FieldType chosen;
            if (!updatedFieldNames.Contains(name) && origByName.TryGetValue(name, out var origField))
                chosen = origField.FieldType;
            else
            {
                var inferred = InferFieldType(value);
                chosen = inferred == FieldType.Null ? FieldType.String : inferred;
            }
            newFields.Add(new Field(name, chosen));
        }
        var newSchema = new Schema(rc.Schema?.Name ?? "record", newFields);

        var updatedRecs = new List<Record>(updatedDicts.Count);
        foreach (var dict in updatedDicts)
        {
            var rec = new Record(newSchema);
            foreach (var (k, v) in dict) rec.SetField(k, v);
            updatedRecs.Add(rec);
        }

        return SingleResult.Rent(FlowFile.WithContent(ff, new RecordContent(newSchema, updatedRecs)));
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
}

/// <summary>
/// SplitRecord: fan out a RecordContent FlowFile into one FlowFile per record.
/// Each output FlowFile carries the original attributes plus per-record
/// attributes <c>split.index</c> and <c>split.total</c> (both zero-padded to
/// the width of the total count for lexical-sort friendliness).
///
/// This is the dataflow analogue of list unfolding: a stream of records
/// becomes a stream of single-record events that can then be routed,
/// transformed, or sunk independently. Pair with <see cref="RouteRecord"/>
/// to route on per-record fields.
/// </summary>
public sealed class SplitRecord : IProcessor
{
    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        var result = MultipleResult.Rent();
        var total = rc.Records.Count;
        var width = total.ToString().Length;
        var totalStr = total.ToString();
        for (int i = 0; i < total; i++)
        {
            // Build each child in a single Rent call with the attribute
            // overlay chained directly on AttributeMap. Chaining
            // FlowFile.WithContent → WithAttribute → WithAttribute here
            // orphans two intermediate FF shells and leaks two AddRefs on
            // the singleton content per child — the new content has
            // refcount 1 but three FF shells end up holding a reference
            // to it, of which only one is ever Returned.
            var singletonContent = new RecordContent(rc.Schema, new List<Record> { rc.Records[i] });
            var childAttrs = ff.Attributes
                .With("split.index", i.ToString().PadLeft(width, '0'))
                .With("split.total", totalStr);
            var child = FlowFile.Rent(ff.NumericId, childAttrs, singletonContent, ff.Timestamp, ff.HopCount);
            result.FlowFiles.Add(child);
        }

        // Release the original FlowFile shell + its (now-unreferenced)
        // RecordContent. Children each own a fresh singleton RecordContent.
        FlowFile.Return(ff);
        return result;
    }
}

/// <summary>
/// RouteRecord: partition each incoming RecordContent's records across named
/// routes based on per-route expression predicates evaluated against the
/// record's fields. First matching route wins; records matching no route are
/// emitted to "unmatched".
///
/// Config key <c>routes</c> is a semicolon-separated list of
/// <c>routeName: expression</c> pairs. Each expression evaluates through
/// <see cref="ExpressionEngine"/> against the record via
/// <see cref="RecordValueResolver"/> — field identifiers are bare names
/// (e.g. <c>age</c>, <c>region</c>) with dotted-path support for nested
/// records.
///
/// Emits one FlowFile per route that received at least one record (plus
/// "unmatched" if any records fell through), each carrying a
/// <see cref="RecordContent"/> with only the matching records and the
/// original schema. The upstream FlowFile is consumed.
///
/// Examples:
///   premium: tier == "gold";
///   minors:  age &lt; 18;
///   adults:  age &gt;= 18
///
/// This is the record-level counterpart to <c>RouteOnAttribute</c>: route on
/// record field values rather than FlowFile attributes, without having to
/// first ExtractRecordField into an attribute.
/// </summary>
public sealed class RouteRecord : IProcessor
{
    private readonly List<(string Route, CompiledExpression Predicate)> _routes;

    public RouteRecord(string routes)
    {
        _routes = new List<(string, CompiledExpression)>();
        if (string.IsNullOrWhiteSpace(routes)) return;
        var entries = routes.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        for (int i = 0; i < entries.Length; i++)
        {
            var entry = entries[i];
            var colonIdx = entry.IndexOf(':');
            if (colonIdx <= 0)
                throw new ConfigException(
                    $"RouteRecord: malformed route at index {i}: '{entry}' — expected 'name: expression'");
            var routeName = entry[..colonIdx].Trim();
            var exprStr = entry[(colonIdx + 1)..].Trim();
            if (string.IsNullOrEmpty(routeName))
                throw new ConfigException(
                    $"RouteRecord: route at index {i} has empty name");
            if (string.IsNullOrEmpty(exprStr))
                throw new ConfigException(
                    $"RouteRecord: route '{routeName}' has empty expression");
            if (string.Equals(routeName, "unmatched", StringComparison.Ordinal))
                throw new ConfigException(
                    "RouteRecord: 'unmatched' is reserved for records that match no route");
            try
            {
                _routes.Add((routeName, ExpressionEngine.Compile(exprStr)));
            }
            catch (FormatException ex)
            {
                throw new ConfigException(
                    $"RouteRecord: route '{routeName}' has invalid expression '{exprStr}': {ex.Message}");
            }
        }
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc)
            return SingleResult.Rent(ff);

        // Partition records by route. Keep per-route buckets in insertion order
        // so downstream FlowFile emission is deterministic (tests + flow replay
        // behave the same across runs).
        var buckets = new Dictionary<string, List<Record>>(StringComparer.Ordinal);
        foreach (var record in rc.Records)
        {
            string? matched = null;
            foreach (var (route, predicate) in _routes)
            {
                EvalValue v;
                // Evaluation errors (divide by zero, missing fields treated as null
                // in a comparison) are treated as "no match" for this route, not
                // a whole-FlowFile failure. Records missing a field referenced by
                // a predicate simply don't match that route.
                try { v = predicate.Eval(new RecordValueResolver(record)); }
                catch { continue; }
                if (v.IsTruthy)
                {
                    matched = route;
                    break;
                }
            }
            var key = matched ?? "unmatched";
            if (!buckets.TryGetValue(key, out var list))
            {
                list = new List<Record>();
                buckets[key] = list;
            }
            list.Add(record);
        }

        // No records at all — drop the empty FlowFile rather than emit empty buckets.
        if (buckets.Count == 0)
        {
            FlowFile.Return(ff);
            return DroppedResult.Instance;
        }

        var result = MultiRoutedResult.Rent();
        foreach (var (route, records) in buckets)
        {
            var content = new RecordContent(rc.Schema, records);
            var outFf = FlowFile.WithContent(ff, content);
            result.Outputs.Add((route, outFf));
        }

        // Return the upstream FlowFile shell; FlowFile.Return releases the
        // original RecordContent (no downstream FlowFile references it —
        // each bucket is wrapped in a fresh RecordContent above).
        FlowFile.Return(ff);

        return result;
    }
}
