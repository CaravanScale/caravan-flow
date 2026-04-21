require "../processor"

# ExtractText — run a regex with capture groups against content; each
# group becomes a FlowFile attribute. Named groups use their name;
# positional groups use the names from `group_names` (comma-separated),
# or fall back to `group.1`, `group.2`, ...
class ExtractText < Processor
  property pattern : String = ""
  property group_names : String = ""

  register "ExtractText",
    description: "Regex capture groups → attributes",
    category: "Text",
    params: {
      pattern: {
        type: "Expression", required: true,
        placeholder: "(?<user>\\w+)@(?<host>\\w+)",
        description: "regex with named or positional capture groups",
      },
      group_names: {
        type: "String",
        placeholder: "user,host",
        description: "comma-separated names for positional groups",
      },
    }

  def process(ff : FlowFile) : Nil
    re = Regex.new(@pattern)
    m = re.match(ff.text)
    if m.nil?
      emit "unmatched", ff
      return
    end
    # Named captures go straight to attrs.
    m.named_captures.each do |name, value|
      ff.attributes[name] = value.to_s if value
    end
    # Positional captures (1-indexed; 0 is the full match).
    names = @group_names.split(',').map(&.strip).reject(&.empty?)
    (1...m.size).each do |i|
      val = m[i]?
      next if val.nil?
      attr_name = names[i - 1]? || "group.#{i}"
      ff.attributes[attr_name] = val
    end
    emit "success", ff
  end
end
