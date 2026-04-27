require "../processor"

# SplitText — split content on a delimiter (supporting `\n`, `\n\n`,
# `\t` escape sequences) into one FlowFile per chunk. Optional
# header-line skip at the start of the original file.
class SplitText < Processor
  property delimiter : String = "\n"
  property header_lines : Int32 = 0

  register "SplitText",
    description: "Split content by delimiter into multiple FlowFiles",
    category: "Text",
    wizard_component: "SplitText",
    params: {
      delimiter: {
        type: "String", required: true, placeholder: "\\n\\n",
        description: "literal separator between records; supports escape sequences",
      },
      header_lines: {
        type: "Int32", default: "0",
        description: "lines to skip at the start of the file",
      },
    }

  private def unescape(s : String) : String
    s.gsub("\\n", "\n").gsub("\\t", "\t").gsub("\\r", "\r")
  end

  def process(ff : FlowFile) : Nil
    text = ff.text
    if @header_lines > 0
      lines = text.split('\n')
      text = lines[@header_lines..]?.try(&.join('\n')) || ""
    end
    delim = unescape(@delimiter)
    chunks = text.split(delim).reject(&.empty?)
    chunks.each_with_index do |chunk, idx|
      child = ff.clone
      child.text = chunk
      child.attributes["fragment.index"] = idx.to_s
      child.attributes["fragment.count"] = chunks.size.to_s
      emit "success", child
    end
  end
end
