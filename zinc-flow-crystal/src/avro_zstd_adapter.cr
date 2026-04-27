require "avro"
require "./zstd"

# Plugs zinc-flow-crystal's Zstd FFI module into crystal-avro's
# codec registry. Without this, OCF files with codec="zstandard" would
# fail to decode. Required once at boot from zinc_flow.cr.
class ZstdAvroCodec
  include Avro::Codec

  def name : String
    "zstandard"
  end

  def compress(bytes : Bytes) : Bytes
    Zstd.compress(bytes)
  end

  def decompress(bytes : Bytes) : Bytes
    Zstd.decompress(bytes)
  end
end

Avro::Codecs.register(ZstdAvroCodec.new)
