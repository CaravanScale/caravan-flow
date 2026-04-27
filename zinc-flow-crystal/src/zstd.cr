# Thin Zstandard FFI. Binds the one-shot compress/decompress entry
# points from libzstd; streaming + dictionary APIs can be added if/when
# a processor needs them. Keeps the bound surface minimal on purpose —
# it's 12 fun declarations instead of a whole shard.
#
# Link: statically against libzstd.a in the Alpine build container,
# dynamically against brew's libzstd for host dev.
@[Link("zstd")]
lib LibZstd
  # Return value of compress/decompress is either the written size OR
  # an error code. is_error() is the only safe way to tell them apart;
  # get_error_name() turns the code into a diagnostic string.
  fun compress = ZSTD_compress(
    dst : Void*, dst_capacity : LibC::SizeT,
    src : Void*, src_size : LibC::SizeT,
    compression_level : LibC::Int,
  ) : LibC::SizeT

  fun decompress = ZSTD_decompress(
    dst : Void*, dst_capacity : LibC::SizeT,
    src : Void*, src_size : LibC::SizeT,
  ) : LibC::SizeT

  fun compress_bound = ZSTD_compressBound(src_size : LibC::SizeT) : LibC::SizeT

  # ZSTD_getFrameContentSize returns either the decompressed size or
  # one of two sentinel values. ZSTD_CONTENTSIZE_ERROR = (size_t)(-2),
  # ZSTD_CONTENTSIZE_UNKNOWN = (size_t)(-1). Callers must check.
  fun get_frame_content_size = ZSTD_getFrameContentSize(
    src : Void*, src_size : LibC::SizeT,
  ) : LibC::ULongLong

  fun is_error = ZSTD_isError(code : LibC::SizeT) : LibC::UInt
  fun get_error_name = ZSTD_getErrorName(code : LibC::SizeT) : LibC::Char*
  fun version_number = ZSTD_versionNumber : LibC::UInt
end

module Zstd
  # Sentinels mirror libzstd's `(size_t)(-1)` / `(size_t)(-2)`. ULongLong
  # is unsigned so we compute via MAX to avoid a signed-to-unsigned cast
  # overflow.
  CONTENTSIZE_UNKNOWN = LibC::ULongLong::MAX
  CONTENTSIZE_ERROR   = LibC::ULongLong::MAX - 1

  # Compression levels Zstd itself accepts 1..22 (ultra-high levels 20-22
  # need ZSTD_compressBound_usingCDict); we cap at 22. Default 3 matches
  # libzstd's own default and is a solid speed/ratio tradeoff.
  LEVEL_FASTEST  =  1
  LEVEL_BALANCED =  3
  LEVEL_SMALLEST = 19

  class Error < Exception; end

  def self.version : String
    n = LibZstd.version_number
    "#{n // 10000}.#{(n // 100) % 100}.#{n % 100}"
  end

  def self.compress(src : Bytes, level : Int32 = LEVEL_BALANCED) : Bytes
    bound = LibZstd.compress_bound(LibC::SizeT.new(src.size))
    dst = Bytes.new(bound)
    written = LibZstd.compress(
      dst.to_unsafe.as(Void*), LibC::SizeT.new(dst.size),
      src.to_unsafe.as(Void*), LibC::SizeT.new(src.size),
      LibC::Int.new(level),
    )
    check!(written)
    dst[0, written.to_i]
  end

  # Decompress a zstd-framed buffer. Requires the input to be a complete
  # frame with a content-size header (set by `compress` above, since
  # libzstd encodes the content size when it's known at compress time).
  # If the frame lacks a size header the decoder falls back to a
  # probe-and-grow loop; we cap that at 64 MiB per call.
  FALLBACK_CAP = 64 * 1024 * 1024

  def self.decompress(src : Bytes) : Bytes
    raise Error.new("empty zstd input") if src.empty?
    size = LibZstd.get_frame_content_size(src.to_unsafe.as(Void*), LibC::SizeT.new(src.size))
    if size == CONTENTSIZE_ERROR
      raise Error.new("not a valid zstd frame")
    end
    capacity = size == CONTENTSIZE_UNKNOWN ? FALLBACK_CAP : size.to_i64
    raise Error.new("decompressed size exceeds #{FALLBACK_CAP} bytes") if capacity > FALLBACK_CAP
    dst = Bytes.new(capacity)
    written = LibZstd.decompress(
      dst.to_unsafe.as(Void*), LibC::SizeT.new(dst.size),
      src.to_unsafe.as(Void*), LibC::SizeT.new(src.size),
    )
    check!(written)
    dst[0, written.to_i]
  end

  private def self.check!(code : LibC::SizeT) : Nil
    return if LibZstd.is_error(code) == 0
    raise Error.new(String.new(LibZstd.get_error_name(code)))
  end
end
