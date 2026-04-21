require "../processor"

# FilterAttribute — `mode` = "remove" | "keep" applied to the attribute
# map against a semicolon-delimited list of attribute names.
class FilterAttribute < Processor
  property mode : String = "remove"
  property attributes : String = ""

  register "FilterAttribute",
    description: "Remove or keep specific attributes",
    category: "Attribute",
    params: {
      mode: {
        type: "Enum", required: true, default: "remove",
        choices: ["remove", "keep"],
        description: "remove = drop listed attributes; keep = drop everything else",
      },
      attributes: {
        type: "StringList", required: true,
        entry_delim: ";",
        placeholder: "http.headers;internal.tmp",
        description: "attribute names to remove or keep",
      },
    }

  def process(ff : FlowFile) : Nil
    names = @attributes.split(';').map(&.strip).reject(&.empty?).to_set
    case @mode
    when "remove"
      names.each { |n| ff.attributes.delete(n) }
    when "keep"
      ff.attributes.select! { |k, _| names.includes?(k) }
    else
      raise "FilterAttribute: mode must be 'remove' or 'keep', got #{@mode}"
    end
    emit "success", ff
  end
end
