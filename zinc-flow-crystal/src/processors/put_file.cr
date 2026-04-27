require "../processor"

# PutFile — write FlowFile content to disk. Filename comes from an
# attribute (default: "filename"); fallback is the flowfile uuid.
class PutFile < Processor
  property output_dir : String = ""
  property naming_attribute : String = "filename"
  property prefix : String = ""
  property suffix : String = ".dat"

  register "PutFile",
    description: "Write FlowFile content to a directory",
    category: "Sink",
    params: {
      output_dir: {
        type: "String", required: true,
        placeholder: "/tmp/zinc-out",
      },
      naming_attribute: {
        type: "String", default: "filename",
        description: "FlowFile attribute that supplies the output filename",
      },
      prefix: {type: "String", default: ""},
      suffix: {type: "String", default: ".dat"},
    }

  def on_start : Nil
    Dir.mkdir_p(@output_dir) if !@output_dir.empty? && !Dir.exists?(@output_dir)
  end

  def process(ff : FlowFile) : Nil
    base = ff.attributes[@naming_attribute]? || ff.uuid
    path = File.join(@output_dir, "#{@prefix}#{base}#{@suffix}")
    File.write(path, ff.content)
    ff.attributes["put.file.path"] = path
    emit "success", ff
  end
end
