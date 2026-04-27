require "./flowfile"

# NiFi FlowFile V3 wire format — pack / unpack binary frames.
#
# Layout:
#   magic         : 7 bytes = "NiFiFF3"
#   attr_count    : field-length
#   for N:
#     key_len     : field-length, then N UTF-8 bytes
#     val_len     : field-length, then N UTF-8 bytes
#   content_len   : 8-byte big-endian Int64
#   content       : content_len bytes
#
# Field-length encoding: 2-byte big-endian UInt16 if < 0xFFFF, else
# two bytes of 0xFF 0xFF followed by a 4-byte big-endian UInt32. This
# two-tier scheme is the NiFi spec; keeps small counts compact.
module FlowFileV3
  extend self

  MAGIC            = Bytes[0x4E, 0x69, 0x46, 0x69, 0x46, 0x46, 0x33]  # "NiFiFF3"
  MAGIC_LEN        = 7
  MAX_2BYTE_LENGTH = 0xFFFF

  class InvalidFrameError < Exception; end

  # Pack one FlowFile (attributes + content) into a V3 frame.
  def pack(ff : FlowFile) : Bytes
    io = IO::Memory.new
    io.write(MAGIC)
    write_field_length(io, ff.attributes.size)
    ff.attributes.each do |k, v|
      kb = k.to_slice
      vb = v.to_slice
      write_field_length(io, kb.size)
      io.write(kb)
      write_field_length(io, vb.size)
      io.write(vb)
    end
    write_i64_be(io, ff.content.size.to_i64)
    io.write(ff.content)
    io.to_slice
  end

  # Pack multiple flowfiles concatenated. UnpackAll walks the stream.
  def pack_multiple(flowfiles : Array(FlowFile)) : Bytes
    io = IO::Memory.new
    flowfiles.each { |ff| io.write(pack(ff)) }
    io.to_slice
  end

  # Unpack all V3 frames from a buffer, stopping at first malformed frame
  # (rather than aborting on the last-byte torn frame case — matches the
  # zinc-csharp sibling's permissive parser).
  def unpack_all(data : Bytes) : Array(FlowFile)
    result = [] of FlowFile
    pos = 0
    while pos < data.size
      ff, new_pos = unpack(data, pos)
      break if ff.nil?
      result << ff
      pos = new_pos
    end
    result
  end

  # Unpack a single frame starting at `offset`. Returns {nil, offset} on
  # magic mismatch or any framing error.
  def unpack(data : Bytes, offset : Int32) : {FlowFile?, Int32}
    return {nil, offset} if offset + MAGIC_LEN > data.size
    MAGIC_LEN.times do |i|
      return {nil, offset} if data[offset + i] != MAGIC[i]
    end
    pos = offset + MAGIC_LEN

    count, pos = read_field_length(data, pos)
    attrs = {} of String => String
    count.times do
      klen, pos = read_field_length(data, pos)
      return {nil, offset} if pos + klen > data.size
      key = String.new(data[pos, klen])
      pos += klen

      vlen, pos = read_field_length(data, pos)
      return {nil, offset} if pos + vlen > data.size
      val = String.new(data[pos, vlen])
      pos += vlen

      attrs[key] = val
    end

    return {nil, offset} if pos + 8 > data.size
    content_len = read_i64_be(data, pos).to_i
    pos += 8
    return {nil, offset} if pos + content_len > data.size
    content = Bytes.new(content_len)
    data[pos, content_len].copy_to(content)
    pos += content_len

    {FlowFile.new(content: content, attributes: attrs), pos}
  end

  # --- low-level helpers ---

  private def write_field_length(io : IO, value : Int32) : Nil
    if value < MAX_2BYTE_LENGTH
      io.write_byte(((value >> 8) & 0xFF).to_u8)
      io.write_byte((value & 0xFF).to_u8)
    else
      io.write_byte(0xFF_u8)
      io.write_byte(0xFF_u8)
      io.write_byte(((value >> 24) & 0xFF).to_u8)
      io.write_byte(((value >> 16) & 0xFF).to_u8)
      io.write_byte(((value >> 8) & 0xFF).to_u8)
      io.write_byte((value & 0xFF).to_u8)
    end
  end

  private def read_field_length(data : Bytes, offset : Int32) : {Int32, Int32}
    raise InvalidFrameError.new("field-length truncated") if offset + 2 > data.size
    val = ((data[offset].to_i32 << 8) | data[offset + 1].to_i32)
    return {val, offset + 2} if val < MAX_2BYTE_LENGTH
    raise InvalidFrameError.new("4-byte length truncated") if offset + 6 > data.size
    big = (data[offset + 2].to_i32 << 24) |
          (data[offset + 3].to_i32 << 16) |
          (data[offset + 4].to_i32 << 8) |
          data[offset + 5].to_i32
    {big, offset + 6}
  end

  private def write_i64_be(io : IO, value : Int64) : Nil
    8.times { |i| io.write_byte(((value >> ((7 - i) * 8)) & 0xFF).to_u8) }
  end

  private def read_i64_be(data : Bytes, offset : Int32) : Int64
    raise InvalidFrameError.new("i64 truncated") if offset + 8 > data.size
    v = 0_i64
    8.times { |i| v = (v << 8) | data[offset + i].to_i64 }
    v
  end
end
