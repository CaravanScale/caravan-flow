using System.Numerics;

namespace ZincFlow.Core;

/// <summary>
/// Conversions between Avro logical-type underlying primitives and the natural
/// CLR types users want to work with (DateTime, DateOnly, TimeOnly, Guid, decimal).
///
/// Storage in Record stays as the underlying primitive (long, int, string,
/// byte[]) so OCF roundtrips preserve byte-for-byte fidelity. These helpers are
/// the seam where business code reads/writes the natural type.
/// </summary>
public static class LogicalTypeHelpers
{
    private static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // --- timestamp-millis (long) ↔ DateTime UTC ---

    public static long ToTimestampMillis(DateTime dt)
        => (long)((dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime()) - UnixEpoch).TotalMilliseconds;

    public static DateTime FromTimestampMillis(long millis)
        => UnixEpoch.AddMilliseconds(millis);

    // --- timestamp-micros (long) ↔ DateTime UTC ---

    public static long ToTimestampMicros(DateTime dt)
    {
        var utc = dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime();
        return (utc - UnixEpoch).Ticks / 10; // 1 tick = 100ns; 1 micro = 1000ns
    }

    public static DateTime FromTimestampMicros(long micros)
        => UnixEpoch.AddTicks(micros * 10);

    // --- date (int) ↔ DateOnly ---

    public static int ToDate(DateOnly d)
        => (d.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc) - UnixEpoch).Days;

    public static DateOnly FromDate(int days)
        => DateOnly.FromDateTime(UnixEpoch.AddDays(days));

    // --- time-millis (int) ↔ TimeOnly ---

    public static int ToTimeMillis(TimeOnly t)
        => (int)(t.ToTimeSpan().TotalMilliseconds);

    public static TimeOnly FromTimeMillis(int millis)
        => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(millis));

    // --- time-micros (long) ↔ TimeOnly ---

    public static long ToTimeMicros(TimeOnly t)
        => t.ToTimeSpan().Ticks / 10;

    public static TimeOnly FromTimeMicros(long micros)
        => TimeOnly.FromTimeSpan(TimeSpan.FromTicks(micros * 10));

    // --- uuid (string) ↔ Guid ---

    public static string ToUuid(Guid g) => g.ToString("D");
    public static Guid FromUuid(string s) => Guid.TryParse(s, out var g) ? g : Guid.Empty;

    // --- decimal (byte[]) ↔ decimal value, given scale ---
    //
    // Avro spec: two's complement big-endian byte representation of the unscaled
    // integer value (BigInteger). The decimal value is unscaled / 10^scale.

    public static byte[] ToDecimalBytes(decimal value, int scale)
    {
        var scaled = value * (decimal)Math.Pow(10, scale);
        var unscaled = new BigInteger(Math.Truncate(scaled));
        var bytes = unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
        return bytes;
    }

    public static decimal FromDecimalBytes(byte[] bytes, int scale)
    {
        if (bytes.Length == 0) return 0m;
        var unscaled = new BigInteger(bytes, isUnsigned: false, isBigEndian: true);
        // Use double for the divisor since BigInteger / decimal isn't direct;
        // for typical financial precision (scale <= 18), this is exact.
        var divisor = (decimal)Math.Pow(10, scale);
        return (decimal)unscaled / divisor;
    }
}
