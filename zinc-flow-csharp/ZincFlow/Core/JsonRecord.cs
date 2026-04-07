using System.Text.Json;

namespace ZincFlow.Core;

// --- JSON RecordReader: JSON array → GenericRecords ---

public sealed class JsonRecordReader : IRecordReader
{
    public List<GenericRecord> Read(byte[] data, Schema schema)
    {
        List<Dictionary<string, object?>>? rawList;
        try
        {
            rawList = JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(data);
        }
        catch
        {
            return [];
        }

        if (rawList is null || rawList.Count == 0) return [];

        // Schema-on-read: infer from first object if schema has no fields
        var effectiveSchema = schema;
        if (schema.Fields.Count == 0)
            effectiveSchema = InferSchema(schema.Name, rawList[0]);

        var records = new List<GenericRecord>(rawList.Count);
        foreach (var raw in rawList)
        {
            var record = new GenericRecord(effectiveSchema);
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
}

// --- JSON RecordWriter: GenericRecords → JSON array ---

public sealed class JsonRecordWriter : IRecordWriter
{
    public byte[] Write(List<GenericRecord> records, Schema schema)
    {
        var rawList = new List<Dictionary<string, object?>>(records.Count);
        foreach (var record in records)
        {
            var raw = new Dictionary<string, object?>();
            foreach (var f in schema.Fields)
            {
                var val = record.GetField(f.Name);
                if (val is not null)
                    raw[f.Name] = val;
            }
            rawList.Add(raw);
        }

        try
        {
            return JsonSerializer.SerializeToUtf8Bytes(rawList);
        }
        catch
        {
            return [];
        }
    }
}
