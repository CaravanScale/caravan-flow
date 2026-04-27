require "compress/gzip"
require "../processor"
require "../zstd"

# CompressContent — gzip via stdlib, zstd via FFI. Stamps two
# attributes so a downstream DecompressContent (or a consumer) can
# pick the right codec:
#   compression.algorithm  — "gzip" | "zstd"
#   compression.level      — the level used (for traceability)
class CompressContent < Processor
  property algorithm : String = "gzip"
  property level : String = "balanced"

  register "CompressContent",
    description: "Gzip- or zstd-compress FlowFile content; stamps compression.* attributes",
    category: "Utility",
    params: {
      algorithm: {
        type: "Enum", required: true, default: "gzip",
        choices: ["gzip", "zstd"],
      },
      level: {
        type: "Enum", default: "balanced",
        choices: ["fastest", "balanced", "smallest"],
      },
    }

  private def gzip_level : Int32
    case @level
    when "fastest"  then Compress::Gzip::BEST_SPEED
    when "smallest" then Compress::Gzip::BEST_COMPRESSION
    else                 Compress::Gzip::DEFAULT_COMPRESSION
    end
  end

  private def zstd_level : Int32
    case @level
    when "fastest"  then Zstd::LEVEL_FASTEST
    when "smallest" then Zstd::LEVEL_SMALLEST
    else                 Zstd::LEVEL_BALANCED
    end
  end

  def process(ff : FlowFile) : Nil
    case @algorithm
    when "gzip"
      io = IO::Memory.new
      Compress::Gzip::Writer.open(io, level: gzip_level) do |gz|
        gz.write ff.content
      end
      ff.content = io.to_slice
    when "zstd"
      ff.content = Zstd.compress(ff.content, zstd_level)
    else
      raise "CompressContent: unknown algorithm #{@algorithm}"
    end
    ff.attributes["compression.algorithm"] = @algorithm
    ff.attributes["compression.level"] = @level
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "compress error"
    emit "failure", ff
  end
end
