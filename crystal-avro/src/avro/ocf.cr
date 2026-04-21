require "random/secure"
require "./schema"
require "./json"
require "./binary"
require "./codec"

module Avro
  # Avro Object Container File (OCF), spec 1.11.
  #
  # Layout:
  #   magic   : 4 bytes     = "Obj\x01"
  #   metadata: map<string, bytes>  (Avro-encoded; avro.schema required,
  #                                  avro.codec optional, defaults to "null")
  #   sync    : 16 bytes    (random, repeated at end of every block)
  #   blocks  : ( count:long, size:long, data:bytes[size], sync:16 )*
  #
  # Writing emits a single block per call — simpler, suits caravan-flow's
  # "one flowfile = one call" model. Reading supports any block count.
  module OCF
    MAGIC = Bytes[0x4F, 0x62, 0x6A, 0x01]  # "Obj\x01"
    SYNC_SIZE = 16

    class InvalidFileError < Exception; end

    struct DecodedFile
      getter schema : Schema
      getter records : Array(Record)
      getter codec_name : String

      def initialize(@schema, @records, @codec_name)
      end
    end

    # Decode an OCF file from bytes. Returns the writer's embedded
    # schema + all records across all blocks, plus the codec name
    # recorded in the file's metadata.
    def self.decode(bytes : Bytes) : DecodedFile
      io = IO::Memory.new(bytes)
      decode_from(io)
    end

    def self.decode_from(io : IO) : DecodedFile
      magic = Bytes.new(MAGIC.size)
      io.read_fully(magic)
      unless magic == MAGIC
        raise InvalidFileError.new("bad OCF magic: expected #{MAGIC.to_a.inspect}, got #{magic.to_a.inspect}")
      end

      metadata = read_metadata(io)
      schema_bytes = metadata["avro.schema"]? ||
        raise InvalidFileError.new("OCF metadata missing avro.schema")
      codec_name = metadata["avro.codec"]?.try { |b| String.new(b) } || "null"

      codec = Codecs.get(codec_name)
      schema = SchemaJson.parse(String.new(schema_bytes))

      sync = Bytes.new(SYNC_SIZE)
      io.read_fully(sync)

      records = [] of Record
      # Read blocks until EOF. Each block ends with a sync marker we
      # verify — a mismatch means corruption or a different file
      # concatenated in (which Avro considers a hard error).
      loop do
        break if peek_eof?(io)
        count = Encoding.read_long(io)
        size = Encoding.read_long(io)
        raise InvalidFileError.new("negative OCF block size: #{size}") if size < 0

        block_bytes = Bytes.new(size)
        io.read_fully(block_bytes)

        trailing_sync = Bytes.new(SYNC_SIZE)
        io.read_fully(trailing_sync)
        unless trailing_sync == sync
          raise InvalidFileError.new("OCF block sync mismatch")
        end

        decoded = codec.decompress(block_bytes)
        block_io = IO::Memory.new(decoded)
        count.times do
          records << BinaryCodec.read_record(block_io, schema)
        end
      end

      DecodedFile.new(schema, records, codec_name)
    end

    # Encode records + schema as OCF bytes. Single-block output, codec
    # from the registry (defaults to "null"). sync is 16 random bytes
    # from Random::Secure so the file fingerprints uniquely even when
    # two files share the same schema + data.
    def self.encode(records : Array(Record), schema : Schema, codec_name : String = "null") : Bytes
      io = IO::Memory.new
      encode_to(io, records, schema, codec_name)
      io.to_slice
    end

    def self.encode_to(io : IO, records : Array(Record), schema : Schema, codec_name : String = "null") : Nil
      codec = Codecs.get(codec_name)

      # Serialize records to raw Avro binary, then compress.
      block_io = IO::Memory.new
      records.each do |r|
        BinaryCodec.write_record(block_io, r, schema)
      end
      raw_block = block_io.to_slice
      compressed = codec.compress(raw_block)

      sync = Random::Secure.random_bytes(SYNC_SIZE)

      io.write(MAGIC)
      write_metadata(io, {
        "avro.schema" => SchemaJson.emit(schema).to_slice,
        "avro.codec"  => codec_name.to_slice,
      })
      io.write(sync)

      Encoding.write_long(io, records.size.to_i64)
      Encoding.write_long(io, compressed.size.to_i64)
      io.write(compressed)
      io.write(sync)
    end

    # --- helpers ---

    # Avro map encoding: a sequence of (count, entries)* blocks
    # terminated by a zero count. Each entry is (string-key,
    # bytes-value). A negative count means a size prefix follows;
    # the decoder skips it (we don't need to seek).
    private def self.read_metadata(io : IO) : Hash(String, Bytes)
      sink = {} of String => Bytes
      loop do
        count = Encoding.read_long(io)
        break if count == 0
        if count < 0
          # byte-size of the next block — consumed but unused
          Encoding.read_long(io)
          count = -count
        end
        count.times do
          k = Encoding.read_string(io)
          v = Encoding.read_bytes(io)
          sink[k] = v
        end
      end
      sink
    end

    private def self.write_metadata(io : IO, metadata : Hash(String, Bytes)) : Nil
      Encoding.write_long(io, metadata.size.to_i64)
      metadata.each do |k, v|
        Encoding.write_string(io, k)
        Encoding.write_bytes(io, v)
      end
      Encoding.write_long(io, 0_i64) # terminator
    end

    # IO doesn't have a portable peek-for-EOF, so read one byte +
    # rewind one if we got it. IO::Memory supports this; other IOs
    # (sockets, compressed streams) may not — but OCF decoding is
    # natural for seekable IOs.
    private def self.peek_eof?(io : IO) : Bool
      case io
      when IO::Memory
        io.pos >= io.size
      else
        # Fallback: try to read 1 byte.
        b = io.read_byte
        return true if b.nil?
        # Non-seekable IOs leave the caller to handle the stolen byte.
        # For OCF we use IO::Memory in practice; this branch is just
        # defensive.
        raise InvalidFileError.new("OCF read from non-seekable IO — use decode_from(IO::Memory)")
      end
    end
  end
end
