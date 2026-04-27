using System.Text;
using ZincFlow.Core;

namespace ZincFlow.StdLib;

/// <summary>
/// Avro schema evolution: given a writer schema W (the one bytes were encoded
/// with) and a reader schema R (what consumer code expects), check whether R
/// can read W and, if so, project decoded records onto R.
///
/// Implemented as two-pass: decode using W (existing OCFReader behavior), then
/// per-record project into R, applying type promotion and defaults. This is
/// less efficient than fused promote-on-read but matches the simplicity of the
/// rest of the codec — and at zinc-flow's per-record granularity the cost is
/// negligible.
///
/// Promotion rules (per Avro 1.11 spec §"Schema Resolution"):
///   int → long, float, double
///   long → float, double
///   float → double
///   string ↔ bytes
/// </summary>
public sealed class SchemaCompatibility
{
    public List<string> Errors { get; } = new();
    public List<string> Warnings { get; } = new();
    public bool IsCompatible => Errors.Count == 0;
    public override string ToString()
        => Errors.Count == 0 && Warnings.Count == 0
            ? "compatible"
            : $"errors=[{string.Join("; ", Errors)}] warnings=[{string.Join("; ", Warnings)}]";
}

public static class SchemaResolver
{
    /// <summary>
    /// Check whether a reader schema can safely consume bytes written with a writer schema.
    /// Returns errors for blocking incompatibilities and warnings for permitted divergences
    /// (writer-only fields dropped on read, reader-only fields filled from defaults).
    /// </summary>
    public static SchemaCompatibility Check(Schema reader, Schema writer)
    {
        var result = new SchemaCompatibility();
        var writerByName = writer.Fields.ToDictionary(f => f.Name);
        var readerNames = new HashSet<string>(reader.Fields.Select(f => f.Name));

        foreach (var rField in reader.Fields)
        {
            if (writerByName.TryGetValue(rField.Name, out var wField))
            {
                if (!IsPromotable(wField.FieldType, rField.FieldType))
                    result.Errors.Add($"field '{rField.Name}': cannot promote {wField.FieldType} → {rField.FieldType}");
            }
            else if (rField.DefaultValue is null)
            {
                result.Errors.Add($"field '{rField.Name}': missing in writer and has no default");
            }
            else
            {
                result.Warnings.Add($"field '{rField.Name}': missing in writer, will use default");
            }
        }

        foreach (var wField in writer.Fields)
        {
            if (!readerNames.Contains(wField.Name))
                result.Warnings.Add($"field '{wField.Name}': writer-only, will be dropped on read");
        }

        return result;
    }

    /// <summary>
    /// Project a record decoded with the writer schema onto the reader schema.
    /// Applies field-by-field promotion and fills missing fields from reader defaults.
    /// Caller must verify Check(reader, writer).IsCompatible first.
    /// </summary>
    public static Record Project(Record source, Schema readerSchema, Schema writerSchema)
    {
        var target = new Record(readerSchema);
        var writerByName = writerSchema.Fields.ToDictionary(f => f.Name);

        foreach (var rField in readerSchema.Fields)
        {
            if (!writerByName.TryGetValue(rField.Name, out var wField))
            {
                // Reader-only field — use default (Check guaranteed it's present).
                target.SetField(rField.Name, rField.DefaultValue);
                continue;
            }

            var sourceVal = source.GetField(rField.Name);
            if (sourceVal is null)
            {
                target.SetField(rField.Name, rField.DefaultValue);
                continue;
            }

            target.SetField(rField.Name, Promote(sourceVal, wField.FieldType, rField.FieldType));
        }

        // Writer-only fields are silently dropped (not added to target).
        return target;
    }

    public static bool IsPromotable(FieldType from, FieldType to)
    {
        if (from == to) return true;
        return (from, to) switch
        {
            (FieldType.Int, FieldType.Long) => true,
            (FieldType.Int, FieldType.Float) => true,
            (FieldType.Int, FieldType.Double) => true,
            (FieldType.Long, FieldType.Float) => true,
            (FieldType.Long, FieldType.Double) => true,
            (FieldType.Float, FieldType.Double) => true,
            (FieldType.String, FieldType.Bytes) => true,
            (FieldType.Bytes, FieldType.String) => true,
            _ => false
        };
    }

    public static object? Promote(object? value, FieldType from, FieldType to)
    {
        if (value is null) return null;
        if (from == to) return value;
        return (from, to) switch
        {
            (FieldType.Int, FieldType.Long) => Convert.ToInt64(value),
            (FieldType.Int, FieldType.Float) => Convert.ToSingle(value),
            (FieldType.Int, FieldType.Double) => Convert.ToDouble(value),
            (FieldType.Long, FieldType.Float) => Convert.ToSingle(value),
            (FieldType.Long, FieldType.Double) => Convert.ToDouble(value),
            (FieldType.Float, FieldType.Double) => Convert.ToDouble(value),
            (FieldType.String, FieldType.Bytes) => Encoding.UTF8.GetBytes(value.ToString() ?? ""),
            (FieldType.Bytes, FieldType.String) => Encoding.UTF8.GetString(value as byte[] ?? Array.Empty<byte>()),
            _ => value
        };
    }
}
