require "./schema"

module Avro
  # A Record is the runtime value form of a schema. Keys are field
  # names; values are the decoded Crystal types. Primitive mappings:
  #   null    -> Nil
  #   boolean -> Bool
  #   int     -> Int32
  #   long    -> Int64
  #   float   -> Float32
  #   double  -> Float64
  #   bytes   -> Bytes
  #   string  -> String
  #   enum    -> Int32         (symbol index)
  #   record  -> Record (which is Hash(String, Value))
  #   array   -> Array(Value)
  #   map     -> Hash(String, Value)
  #
  # Value is recursive via Array / Hash (same pattern as JSON::Any::Type),
  # so nested records, arrays-of-records, maps-of-records all type-check.
  alias Value = Nil | Bool | Int32 | Int64 | Float32 | Float64 | Bytes | String | Array(Value) | Hash(String, Value)

  alias Record = Hash(String, Value)

  # Low-level encoding primitives. Static methods instead of class
  # state — the Avro encoding is purely positional, so each primitive
  # is self-contained. Zigzag + 7-bit varint as per Avro 1.11 §3.
  module Encoding
    extend self

    # Zigzag maps signed ints to unsigned so small negatives use few
    # varint bytes: (-1, 0, 1, -2) -> (1, 0, 2, 3).
    def zigzag_encode(n : Int64) : UInt64
      ((n << 1) ^ (n >> 63)).to_u64!
    end

    def zigzag_decode(n : UInt64) : Int64
      ((n >> 1).to_i64! ^ -(n & 1).to_i64!)
    end

    def write_varint(io : IO, value : Int64) : Nil
      v = zigzag_encode(value)
      while (v & ~0x7F_u64) != 0
        io.write_byte(((v & 0x7F_u64) | 0x80_u64).to_u8)
        v >>= 7
      end
      io.write_byte(v.to_u8)
    end

    def read_varint(io : IO) : Int64
      v = 0_u64
      shift = 0
      loop do
        b = io.read_byte
        raise CodecError.new("varint truncated") if b.nil?
        v |= (b & 0x7F).to_u64 << shift
        shift += 7
        break if (b & 0x80) == 0
        raise CodecError.new("varint overflow (>64 bits)") if shift >= 64
      end
      zigzag_decode(v)
    end

    def write_boolean(io : IO, v : Bool) : Nil
      io.write_byte(v ? 1_u8 : 0_u8)
    end

    def read_boolean(io : IO) : Bool
      b = io.read_byte
      raise CodecError.new("boolean truncated") if b.nil?
      b != 0
    end

    def write_int(io : IO, v : Int32) : Nil
      write_varint(io, v.to_i64)
    end

    def read_int(io : IO) : Int32
      read_varint(io).to_i32
    end

    def write_long(io : IO, v : Int64) : Nil
      write_varint(io, v)
    end

    def read_long(io : IO) : Int64
      read_varint(io)
    end

    # IEEE 754 little-endian, per Avro 1.11 §3.
    def write_float(io : IO, v : Float32) : Nil
      io.write_bytes(v, IO::ByteFormat::LittleEndian)
    end

    def read_float(io : IO) : Float32
      io.read_bytes(Float32, IO::ByteFormat::LittleEndian)
    end

    def write_double(io : IO, v : Float64) : Nil
      io.write_bytes(v, IO::ByteFormat::LittleEndian)
    end

    def read_double(io : IO) : Float64
      io.read_bytes(Float64, IO::ByteFormat::LittleEndian)
    end

    # String / bytes: long prefix (byte count) followed by raw bytes.
    def write_bytes(io : IO, bytes : Bytes) : Nil
      write_long(io, bytes.size.to_i64)
      io.write(bytes)
    end

    def read_bytes(io : IO) : Bytes
      len = read_long(io)
      raise CodecError.new("negative bytes length: #{len}") if len < 0
      buf = Bytes.new(len)
      io.read_fully(buf)
      buf
    end

    def write_string(io : IO, str : String) : Nil
      bytes = str.to_slice
      write_long(io, bytes.size.to_i64)
      io.write(bytes)
    end

    def read_string(io : IO) : String
      len = read_long(io)
      raise CodecError.new("negative string length: #{len}") if len < 0
      buf = Bytes.new(len)
      io.read_fully(buf)
      String.new(buf)
    end
  end

  # Encode / decode whole Records given a Schema. Writing is schema-
  # driven: each field's type determines the sub-codec. Reading is
  # identical — we consume bytes in the field order declared by the
  # schema and produce a typed Record.
  #
  # Caveats tracked against the spec:
  #   - Array / Map blocks always written as a single positive-count
  #     block + terminator. Readers accept multi-block streams and the
  #     negative-count-with-size-prefix variant (per Avro §3.2).
  #   - Decimal logical type: bytes + precision/scale surfaced via the
  #     schema, but left as raw Bytes in the Record. Callers convert
  #     to BigDecimal etc. as needed.
  class BinaryCodec
    def self.write_record(io : IO, record : Record, schema : Schema) : Nil
      schema.fields.each do |field|
        write_field(io, record[field.name]?, field)
      end
    end

    def self.read_record(io : IO, schema : Schema) : Record
      rec = Record.new
      schema.fields.each do |field|
        rec[field.name] = read_field(io, field)
      end
      rec
    end

    private def self.write_field(io : IO, v : Value, field : Field) : Nil
      # Nullable = union of [null, T]; write a 0-index for nil, 1 for
      # the actual branch (+ value).
      if field.nullable
        if v.nil?
          Encoding.write_long(io, 0_i64)
          return
        end
        Encoding.write_long(io, 1_i64)
      end
      write_by_primitive(io, v, field)
    end

    private def self.read_field(io : IO, field : Field) : Value
      if field.nullable
        branch = Encoding.read_long(io)
        return nil if branch == 0
      end
      read_by_primitive(io, field)
    end

    private def self.write_by_primitive(io : IO, v : Value, field : Field) : Nil
      case field.type
      when PrimitiveType::Null
        # 0 bytes on the wire
      when PrimitiveType::Boolean
        Encoding.write_boolean(io, v.as(Bool))
      when PrimitiveType::Int
        Encoding.write_int(io, coerce_int32(v))
      when PrimitiveType::Long
        Encoding.write_long(io, coerce_int64(v))
      when PrimitiveType::Float
        Encoding.write_float(io, coerce_float32(v))
      when PrimitiveType::Double
        Encoding.write_double(io, coerce_float64(v))
      when PrimitiveType::Bytes
        Encoding.write_bytes(io, coerce_bytes(v))
      when PrimitiveType::String
        Encoding.write_string(io, v.as(String))
      when PrimitiveType::Enum
        Encoding.write_int(io, coerce_int32(v))
      when PrimitiveType::Record
        rs = field.record_schema || raise CodecError.new("record field '#{field.name}' missing schema")
        write_record(io, v.as(Record), rs)
      when PrimitiveType::Array
        item_field = field.element_type || raise CodecError.new("array '#{field.name}' missing item spec")
        arr = v.as(Array)
        if arr.empty?
          Encoding.write_long(io, 0_i64)
          return
        end
        Encoding.write_long(io, arr.size.to_i64)
        arr.each do |item|
          write_by_primitive(io, item.as(Value), item_field)
        end
        Encoding.write_long(io, 0_i64)
      when PrimitiveType::Map
        vfield = field.values_type || raise CodecError.new("map '#{field.name}' missing values spec")
        h = v.as(Hash)
        if h.empty?
          Encoding.write_long(io, 0_i64)
          return
        end
        Encoding.write_long(io, h.size.to_i64)
        h.each do |k, val|
          Encoding.write_string(io, k.as(String))
          write_by_primitive(io, val.as(Value), vfield)
        end
        Encoding.write_long(io, 0_i64)
      else
        raise CodecError.new("unsupported encode type: #{field.type}")
      end
    end

    private def self.read_by_primitive(io : IO, field : Field) : Value
      case field.type
      when PrimitiveType::Null    then nil
      when PrimitiveType::Boolean then Encoding.read_boolean(io)
      when PrimitiveType::Int     then Encoding.read_int(io)
      when PrimitiveType::Long    then Encoding.read_long(io)
      when PrimitiveType::Float   then Encoding.read_float(io)
      when PrimitiveType::Double  then Encoding.read_double(io)
      when PrimitiveType::Bytes   then Encoding.read_bytes(io)
      when PrimitiveType::String  then Encoding.read_string(io)
      when PrimitiveType::Enum    then Encoding.read_int(io)
      when PrimitiveType::Record
        rs = field.record_schema || raise CodecError.new("record field '#{field.name}' missing schema")
        read_record(io, rs)
      when PrimitiveType::Array
        item_field = field.element_type || raise CodecError.new("array '#{field.name}' missing item spec")
        read_array_block(io, item_field)
      when PrimitiveType::Map
        vfield = field.values_type || raise CodecError.new("map '#{field.name}' missing values spec")
        read_map_block(io, vfield)
      else
        raise CodecError.new("unsupported decode type: #{field.type}")
      end
    end

    private def self.read_array_block(io : IO, item_field : Field) : Array(Value)
      sink = [] of Value
      loop do
        count = Encoding.read_long(io)
        break if count == 0
        if count < 0
          # Negative count signals a size-prefixed block; we don't use
          # the size but must consume it to advance the reader.
          Encoding.read_long(io)
          count = -count
        end
        count.times do
          sink << read_by_primitive(io, item_field)
        end
      end
      sink
    end

    private def self.read_map_block(io : IO, values_field : Field) : Hash(String, Value)
      sink = {} of String => Value
      loop do
        count = Encoding.read_long(io)
        break if count == 0
        if count < 0
          Encoding.read_long(io)
          count = -count
        end
        count.times do
          k = Encoding.read_string(io)
          sink[k] = read_by_primitive(io, values_field)
        end
      end
      sink
    end

    # Coercions: processors often hand us values loosely typed (e.g.
    # a String "42" when the schema wants an int). We accept what's
    # sensible, fail loudly on what isn't.

    private def self.coerce_int32(v : Value) : Int32
      case v
      when Int32   then v
      when Int64   then v.to_i32
      when String  then v.to_i32
      when Bool    then v ? 1 : 0
      when Nil     then 0
      else raise CodecError.new("cannot coerce #{v.class} to Int32")
      end
    end

    private def self.coerce_int64(v : Value) : Int64
      case v
      when Int64   then v
      when Int32   then v.to_i64
      when String  then v.to_i64
      when Bool    then v ? 1_i64 : 0_i64
      when Nil     then 0_i64
      else raise CodecError.new("cannot coerce #{v.class} to Int64")
      end
    end

    private def self.coerce_float32(v : Value) : Float32
      case v
      when Float32 then v
      when Float64 then v.to_f32
      when Int32   then v.to_f32
      when Int64   then v.to_f32
      when String  then v.to_f32
      when Nil     then 0.0_f32
      else raise CodecError.new("cannot coerce #{v.class} to Float32")
      end
    end

    private def self.coerce_float64(v : Value) : Float64
      case v
      when Float64 then v
      when Float32 then v.to_f64
      when Int32   then v.to_f64
      when Int64   then v.to_f64
      when String  then v.to_f64
      when Nil     then 0.0
      else raise CodecError.new("cannot coerce #{v.class} to Float64")
      end
    end

    private def self.coerce_bytes(v : Value) : Bytes
      case v
      when Bytes   then v
      when String  then v.to_slice
      when Nil     then Bytes.empty
      else raise CodecError.new("cannot coerce #{v.class} to Bytes")
      end
    end
  end
end
