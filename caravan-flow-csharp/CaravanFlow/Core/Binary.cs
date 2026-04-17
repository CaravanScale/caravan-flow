using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace CaravanFlow.Core;

/// <summary>
/// Big-endian binary encoding helpers for FlowFile V3 wire format.
/// Uses BinaryPrimitives for SIMD-optimized byte swapping.
/// </summary>
public static class BinaryHelpers
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt16BE(Span<byte> dest, ushort value)
        => BinaryPrimitives.WriteUInt16BigEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort ReadUInt16BE(ReadOnlySpan<byte> data)
        => BinaryPrimitives.ReadUInt16BigEndian(data);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteUInt32BE(Span<byte> dest, uint value)
        => BinaryPrimitives.WriteUInt32BigEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint ReadUInt32BE(ReadOnlySpan<byte> data)
        => BinaryPrimitives.ReadUInt32BigEndian(data);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void WriteInt64BE(Span<byte> dest, long value)
        => BinaryPrimitives.WriteInt64BigEndian(dest, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64BE(ReadOnlySpan<byte> data)
        => BinaryPrimitives.ReadInt64BigEndian(data);
}
