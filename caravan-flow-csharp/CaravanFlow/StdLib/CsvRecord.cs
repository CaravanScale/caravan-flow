using System.Globalization;
using System.Text;
using CaravanFlow.Core;

namespace CaravanFlow.StdLib;

/// <summary>
/// CSV RecordReader: parses CSV bytes into GenericRecords.
/// Handles quoted fields (RFC 4180), configurable delimiter, optional header row.
/// </summary>
public sealed class CsvRecordReader : IRecordReader
{
    private readonly char _delimiter;
    private readonly bool _hasHeader;

    public CsvRecordReader(char delimiter = ',', bool hasHeader = true)
    {
        _delimiter = delimiter;
        _hasHeader = hasHeader;
    }

    public List<GenericRecord> Read(byte[] data, Schema schema)
    {
        if (data.Length == 0) return [];

        var text = Encoding.UTF8.GetString(data);
        var lines = ParseLines(text);
        if (lines.Count == 0) return [];

        // Determine field names
        List<string> headers;
        int dataStart;
        if (_hasHeader)
        {
            headers = ParseFields(lines[0]);
            dataStart = 1;
        }
        else if (schema.Fields.Count > 0)
        {
            headers = schema.Fields.Select(f => f.Name).ToList();
            dataStart = 0;
        }
        else
        {
            // Auto-generate column names: col0, col1, ...
            var firstRow = ParseFields(lines[0]);
            headers = Enumerable.Range(0, firstRow.Count).Select(i => $"col{i}").ToList();
            dataStart = 0;
        }

        // Build schema if not provided
        var effectiveSchema = schema.Fields.Count > 0
            ? schema
            : new Schema(schema.Name, headers.Select(h => new Field(h, FieldType.String)).ToList());

        // Map header position → declared FieldType from the schema (when supplied).
        // Used to coerce raw CSV strings into the right CLR type (Long/Double/etc.)
        // so downstream processors and writers (Avro, OCF, JSON) preserve type.
        var fieldTypes = new FieldType[headers.Count];
        for (int i = 0; i < headers.Count; i++)
        {
            var match = effectiveSchema.Fields.FirstOrDefault(f => f.Name == headers[i]);
            fieldTypes[i] = match?.FieldType ?? FieldType.String;
        }

        var records = new List<GenericRecord>(lines.Count - dataStart);
        for (int i = dataStart; i < lines.Count; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;
            var fields = ParseFields(lines[i]);
            var record = new GenericRecord(effectiveSchema);
            for (int j = 0; j < Math.Min(headers.Count, fields.Count); j++)
                record.SetField(headers[j], CoerceValue(fields[j], fieldTypes[j]));
            records.Add(record);
        }
        return records;
    }

    /// <summary>
    /// Parse a CSV cell into the declared field type. CSV-empty cells become null
    /// (so downstream defaults/coalesce work). Parse failures fall back to the
    /// raw string so data isn't silently dropped.
    /// </summary>
    private static object? CoerceValue(string raw, FieldType type)
    {
        if (type == FieldType.String) return raw;
        if (string.IsNullOrEmpty(raw)) return null;
        return type switch
        {
            FieldType.Boolean => bool.TryParse(raw, out var b) ? b : raw,
            FieldType.Int => int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i) ? i : raw,
            FieldType.Long => long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l) ? l : raw,
            FieldType.Float => float.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var f) ? f : raw,
            FieldType.Double => double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) ? d : raw,
            FieldType.Bytes => Encoding.UTF8.GetBytes(raw),
            _ => raw
        };
    }

    private List<string> ParseFields(string line)
    {
        var fields = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;
        int i = 0;

        while (i < line.Length)
        {
            char c = line[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.Length && line[i + 1] == '"')
                    {
                        sb.Append('"'); // escaped quote
                        i += 2;
                    }
                    else
                    {
                        inQuotes = false;
                        i++;
                    }
                }
                else
                {
                    sb.Append(c);
                    i++;
                }
            }
            else if (c == '"')
            {
                inQuotes = true;
                i++;
            }
            else if (c == _delimiter)
            {
                fields.Add(sb.ToString());
                sb.Clear();
                i++;
            }
            else
            {
                sb.Append(c);
                i++;
            }
        }
        fields.Add(sb.ToString());
        return fields;
    }

    private static List<string> ParseLines(string text)
    {
        // Handle quoted fields that span lines
        var lines = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (c == '"')
                inQuotes = !inQuotes;

            if (!inQuotes && (c == '\n' || c == '\r'))
            {
                if (sb.Length > 0)
                {
                    lines.Add(sb.ToString());
                    sb.Clear();
                }
                // Skip \r\n as single line break
                if (c == '\r' && i + 1 < text.Length && text[i + 1] == '\n')
                    i++;
            }
            else
            {
                sb.Append(c);
            }
        }
        if (sb.Length > 0)
            lines.Add(sb.ToString());

        return lines;
    }
}

/// <summary>
/// CSV RecordWriter: serializes GenericRecords to CSV bytes.
/// Quotes fields that contain the delimiter, quotes, or newlines.
/// </summary>
public sealed class CsvRecordWriter : IRecordWriter
{
    private readonly char _delimiter;
    private readonly bool _includeHeader;

    public CsvRecordWriter(char delimiter = ',', bool includeHeader = true)
    {
        _delimiter = delimiter;
        _includeHeader = includeHeader;
    }

    public byte[] Write(List<GenericRecord> records, Schema schema)
    {
        if (records.Count == 0) return [];

        var sb = new StringBuilder();

        if (_includeHeader)
        {
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                if (i > 0) sb.Append(_delimiter);
                sb.Append(QuoteField(schema.Fields[i].Name));
            }
            sb.Append('\n');
        }

        foreach (var record in records)
        {
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                if (i > 0) sb.Append(_delimiter);
                var val = record.GetField(schema.Fields[i].Name);
                sb.Append(QuoteField(val?.ToString() ?? ""));
            }
            sb.Append('\n');
        }

        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private string QuoteField(string value)
    {
        if (value.Contains(_delimiter) || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }
}
