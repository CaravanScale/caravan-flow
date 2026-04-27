using YamlDotNet.Core;
using YamlDotNet.Core.Events;

namespace ZincFlow.Core;

/// <summary>
/// AOT-safe YAML parser. Uses YamlDotNet's low-level IParser (token-based,
/// no reflection) to parse YAML into Dictionary trees. Replaces the
/// reflection-based DeserializerBuilder which crashes under AOT trimming.
/// </summary>
public static class YamlParser
{
    /// <summary>
    /// Parse a YAML string into a Dictionary tree.
    /// Supports nested mappings, sequences, and scalar values.
    /// </summary>
    public static Dictionary<string, object?> Parse(string yaml)
    {
        var parser = new Parser(new StringReader(yaml));
        parser.Consume<StreamStart>();
        if (parser.Accept<StreamEnd>(out _))
            return new();
        parser.Consume<DocumentStart>();
        var result = ReadMapping(parser);
        if (parser.Accept<DocumentEnd>(out _))
            parser.Consume<DocumentEnd>();
        return result;
    }

    private static Dictionary<string, object?> ReadMapping(IParser parser)
    {
        var dict = new Dictionary<string, object?>();
        parser.Consume<MappingStart>();
        while (!parser.TryConsume<MappingEnd>(out _))
        {
            var key = parser.Consume<Scalar>().Value;
            dict[key] = ReadValue(parser);
        }
        return dict;
    }

    private static object? ReadValue(IParser parser)
    {
        if (parser.Accept<MappingStart>(out _))
            return ReadMapping(parser);
        if (parser.Accept<SequenceStart>(out _))
            return ReadSequence(parser);
        if (parser.TryConsume<Scalar>(out var scalar))
            return ParseScalar(scalar.Value);
        // Skip unknown events
        parser.MoveNext();
        return null;
    }

    private static List<object?> ReadSequence(IParser parser)
    {
        var list = new List<object?>();
        parser.Consume<SequenceStart>();
        while (!parser.TryConsume<SequenceEnd>(out _))
            list.Add(ReadValue(parser));
        return list;
    }

    private static object? ParseScalar(string value)
    {
        if (string.IsNullOrEmpty(value) || value == "~" || value == "null")
            return null;
        if (value == "true") return true;
        if (value == "false") return false;
        if (int.TryParse(value, out var i)) return i;
        if (long.TryParse(value, out var l)) return l;
        if (double.TryParse(value, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var d)) return d;
        return value;
    }
}
