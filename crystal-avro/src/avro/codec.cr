require "compress/deflate"
require "./schema"

module Avro
  # Block codec interface. OCF files encode blocks through one of these;
  # users can register custom codecs (e.g. zstandard via an FFI shard)
  # by implementing the three methods and calling Codecs.register.
  module Codec
    abstract def name : String
    abstract def compress(bytes : Bytes) : Bytes
    abstract def decompress(bytes : Bytes) : Bytes
  end

  # Codec registry. Pre-populated with "null" and "deflate" (both
  # resolvable with zero external dependencies). Call Codecs.register
  # at boot to add "zstandard", "snappy", etc. from a separate shard.
  module Codecs
    extend self

    @@registry = {} of String => Codec

    def register(codec : Codec) : Nil
      @@registry[codec.name] = codec
    end

    def get(name : String) : Codec
      @@registry[name]? || raise CodecError.new("no codec registered for '#{name}' — did you forget to `Avro::Codecs.register(...)` it?")
    end

    def known?(name : String) : Bool
      @@registry.has_key?(name)
    end

    def names : Array(String)
      @@registry.keys
    end

    # --- built-ins ---

    class NullCodec
      include Codec
      def name : String; "null"; end
      def compress(bytes : Bytes) : Bytes; bytes; end
      def decompress(bytes : Bytes) : Bytes; bytes; end
    end

    class DeflateCodec
      include Codec
      def name : String; "deflate"; end

      def compress(bytes : Bytes) : Bytes
        io = IO::Memory.new
        # Raw deflate stream (no zlib/gzip header) is what Avro OCF
        # expects; that's the stdlib default when the writer is given
        # a plain IO.
        Compress::Deflate::Writer.open(io) { |w| w.write bytes }
        io.to_slice
      end

      def decompress(bytes : Bytes) : Bytes
        src = IO::Memory.new(bytes)
        sink = IO::Memory.new
        Compress::Deflate::Reader.open(src) { |r| IO.copy(r, sink) }
        sink.to_slice
      end
    end

    register(NullCodec.new)
    register(DeflateCodec.new)
  end
end
