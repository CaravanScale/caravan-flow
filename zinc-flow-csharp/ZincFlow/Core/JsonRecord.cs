using System.Text.Json;

namespace ZincFlow.Core;

// --- JSON RecordReader: JSON array → Records ---

public sealed class JsonRecordReader : IRecordReader
{
    public List<Record> Read(byte[] data, Schema schema)
    {
        if (data.Length == 0) return [];

        // Peek first non-whitespace byte to determine JSON shape
        int firstChar = -1;
        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] != ' ' && data[i] != '\t' && data[i] != '\n' && data[i] != '\r')
            {
                firstChar = data[i];
                break;
            }
        }
        if (firstChar < 0) return [];

        List<Dictionary<string, object?>>? rawList;
        if (firstChar == '[')
        {
            if (!TryDeserializeArray(data, out rawList))
                return [];
        }
        else if (firstChar == '{')
        {
            if (!TryDeserializeObject(data, out var single))
                return [];
            rawList = single is not null ? [single] : null;
        }
        else
        {
            return [];
        }

        if (rawList is null || rawList.Count == 0) return [];

        // Schema-on-read: infer from first object if schema has no fields
        var effectiveSchema = schema;
        if (schema.Fields.Count == 0)
            effectiveSchema = InferSchema(schema.Name, rawList[0]);

        var records = new List<Record>(rawList.Count);
        foreach (var raw in rawList)
        {
            var record = new Record(effectiveSchema);
            foreach (var f in effectiveSchema.Fields)
            {
                if (raw.TryGetValue(f.Name, out var val))
                    record.SetField(f.Name, UnwrapJsonElement(val));
                else if (f.DefaultValue is not null)
                    record.SetField(f.Name, f.DefaultValue);
            }
            records.Add(record);
        }
        return records;
    }

    private static object? UnwrapJsonElement(object? val)
    {
        if (val is JsonElement je)
        {
            return je.ValueKind switch
            {
                JsonValueKind.String => je.GetString(),
                JsonValueKind.Number => je.TryGetInt64(out var l) ? l : je.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => je.ToString()
            };
        }
        return val;
    }

    public static Schema InferSchema(string name, Dictionary<string, object?> sample)
    {
        var fields = new List<Field>(sample.Count);
        foreach (var (key, val) in sample)
            fields.Add(new Field(key, InferFieldType(val)));
        return new Schema(name, fields);
    }

    private static FieldType InferFieldType(object? val) => val switch
    {
        null => FieldType.Null,
        JsonElement je => je.ValueKind switch
        {
            JsonValueKind.String => FieldType.String,
            JsonValueKind.Number => FieldType.Double,
            JsonValueKind.True or JsonValueKind.False => FieldType.Boolean,
            _ => FieldType.String
        },
        string => FieldType.String,
        bool => FieldType.Boolean,
        int => FieldType.Int,
        long => FieldType.Long,
        float => FieldType.Float,
        double => FieldType.Double,
        _ => FieldType.String
    };

    private static bool TryDeserializeArray(byte[] data, out List<Dictionary<string, object?>>? result)
    {
        try { result = JsonSerializer.Deserialize(data, ZincJsonContext.Default.ListDictionaryStringObject); return true; }
        catch { result = null; return false; }
    }

    private static bool TryDeserializeObject(byte[] data, out Dictionary<string, object?>? result)
    {
        try { result = JsonSerializer.Deserialize(data, ZincJsonContext.Default.DictionaryStringObject); return true; }
        catch { result = null; return false; }
    }
}

// --- JSON RecordWriter: Records → JSON array ---

public sealed class JsonRecordWriter : IRecordWriter
{
    public byte[] Write(List<Record> records, Schema? schema)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
        {
            writer.WriteStartArray();
            foreach (var record in records)
            {
                writer.WriteStartObject();
                if (schema is not null)
                {
                    // Use declared schema order — deterministic output, aligns with
                    // downstream expectations when a schema is present.
                    foreach (var f in schema.Fields)
                    {
                        var val = record.GetField(f.Name);
                        if (val is null) continue;
                        writer.WritePropertyName(f.Name);
                        ZincJson.WriteValue(writer, val);
                    }
                }
                else
                {
                    // Schemaless: emit whatever fields the record carries in
                    // insertion order.
                    foreach (var (name, val) in record._values)
                    {
                        if (val is null) continue;
                        writer.WritePropertyName(name);
                        ZincJson.WriteValue(writer, val);
                    }
                }
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return ms.ToArray();
    }
}
