require "../processor"

# GetFile — polls a directory on each source_tick; each matched file
# becomes a FlowFile and is deleted from disk. Glob pattern filters by
# extension / name. Keeps a simple in-memory "seen" set so re-polls
# within the same tick don't double-emit (in case the fabric ticks
# faster than we can delete).
class GetFile < Processor
  property input_dir : String = ""
  property pattern : String = "*"
  property delete_after : Bool = true

  register "GetFile",
    description: "Polls a directory; emits one FlowFile per matching file",
    category: "Source",
    kind: "source",
    params: {
      input_dir: {type: "String", required: true, placeholder: "/tmp/zinc-in"},
      pattern:   {type: "String", default: "*", description: "glob pattern for files to pick up"},
      delete_after: {type: "Bool", default: "true", description: "delete file from disk after ingest"},
    }

  # Sources are tick-driven; no inbound process() path.
  def process(ff : FlowFile) : Nil
  end

  def source_tick : Array(FlowFile)
    return [] of FlowFile unless Dir.exists?(@input_dir)
    out = [] of FlowFile
    Dir.glob(File.join(@input_dir, @pattern)).each do |path|
      next unless File.file?(path)
      begin
        bytes = File.open(path, "rb") { |f| f.getb_to_end }
        ff = FlowFile.new(
          content: bytes,
          attributes: {
            "filename"         => File.basename(path),
            "path"             => path,
            "source.processor" => "GetFile",
          },
        )
        out << ff
        File.delete(path) if @delete_after
      rescue e
        STDERR.puts "[GetFile] skipping #{path}: #{e.message}"
      end
    end
    out
  end
end
