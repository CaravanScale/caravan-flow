using System.Text.RegularExpressions;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

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
/// Config: operations as comma-separated directives.
///
/// Directives:
///   rename:oldName:newName   — rename a field
///   remove:fieldName         — remove a field
///   add:fieldName:value      — add a field with a literal value
///   copy:source:target       — copy field value to new field
///   toUpper:fieldName        — uppercase a string field
///   toLower:fieldName        — lowercase a string field
///   default:fieldName:value  — set field to value if null/missing
/// </summary>
public sealed class TransformRecord : IProcessor
{
    private readonly List<(string Op, string Arg1, string Arg2)> _operations;

    public TransformRecord(string operations)
    {
        _operations = ParseOperations(operations);
    }

    public ProcessorResult Process(FlowFile ff)
    {
        if (ff.Content is not RecordContent rc || rc.Records.Count == 0)
            return SingleResult.Rent(ff);

        // Collect all field names that will exist after transformations
        // Start with existing schema fields, then apply add/rename/remove to determine final fields
        var baseFieldNames = rc.Schema.Fields.Select(f => f.Name).ToList();
        var finalFieldNames = new List<string>(baseFieldNames);
        foreach (var (op, arg1, arg2) in _operations)
        {
            switch (op)
            {
                case "rename":
                    finalFieldNames.Remove(arg1);
                    if (!finalFieldNames.Contains(arg2)) finalFieldNames.Add(arg2);
                    break;
                case "remove":
                    finalFieldNames.Remove(arg1);
                    break;
                case "add":
                    if (!finalFieldNames.Contains(arg1)) finalFieldNames.Add(arg1);
                    break;
                case "copy":
                    if (!finalFieldNames.Contains(arg2)) finalFieldNames.Add(arg2);
                    break;
                case "default":
                    if (!finalFieldNames.Contains(arg1)) finalFieldNames.Add(arg1);
                    break;
            }
        }

        var newSchema = new Schema(rc.Schema.Name, finalFieldNames.Select(n => new Field(n, FieldType.String)).ToList());

        var transformed = new List<GenericRecord>(rc.Records.Count);
        foreach (var record in rc.Records)
        {
            // Work on a mutable dictionary copy, then build GenericRecord at the end
            var dict = record.ToDictionary();
            foreach (var (op, arg1, arg2) in _operations)
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
                }
            }
            var newRecord = new GenericRecord(newSchema);
            foreach (var (k, v) in dict)
                newRecord.SetField(k, v);
            transformed.Add(newRecord);
        }

        var updated = FlowFile.WithContent(ff, new RecordContent(newSchema, transformed));
        return SingleResult.Rent(updated);
    }

    private static List<(string, string, string)> ParseOperations(string ops)
    {
        var result = new List<(string, string, string)>();
        if (string.IsNullOrWhiteSpace(ops)) return result;
        foreach (var directive in ops.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = directive.Split(':', 3);
            if (parts.Length < 2) continue;
            result.Add((parts[0], parts[1], parts.Length > 2 ? parts[2] : ""));
        }
        return result;
    }
}
