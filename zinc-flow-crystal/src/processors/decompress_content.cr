require "compress/gzip"
require "../processor"
require "../zstd"

# DecompressContent — the inverse of CompressContent. When
# algorithm="auto" it reads the compression.algorithm attribute set
# by a sibling CompressContent; otherwise it uses the explicit choice.
# Leaves the flowfile's content replaced with the decompressed bytes
# and clears the compression.* attributes.
class DecompressContent < Processor
  property algorithm : String = "auto"

  register "DecompressContent",
    description: "Decompress FlowFile content; auto reads compression.algorithm attribute",
    category: "Utility",
    params: {
      algorithm: {
        type: "Enum", default: "auto",
        choices: ["auto", "gzip", "zstd"],
        description: "auto follows the compression.algorithm attribute set by CompressContent",
      },
    }

  def process(ff : FlowFile) : Nil
    algo =
      if @algorithm == "auto"
        ff.attributes["compression.algorithm"]? || raise "DecompressContent: algorithm=auto but compression.algorithm attribute missing"
      else
        @algorithm
      end
    case algo
    when "gzip"
      io = IO::Memory.new(ff.content)
      sink = IO::Memory.new
      Compress::Gzip::Reader.open(io) { |gz| IO.copy(gz, sink) }
      ff.content = sink.to_slice
    when "zstd"
      ff.content = Zstd.decompress(ff.content)
    else
      raise "DecompressContent: unknown algorithm #{algo}"
    end
    ff.attributes.delete("compression.algorithm")
    ff.attributes.delete("compression.level")
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "decompress error"
    emit "failure", ff
  end
end
