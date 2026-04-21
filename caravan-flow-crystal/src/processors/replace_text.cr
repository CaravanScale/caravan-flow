require "../processor"

# ReplaceText — regex find-and-replace on FlowFile content. mode=all
# replaces every match, mode=first replaces only the first. Uses
# Crystal's PCRE-backed Regex.
class ReplaceText < Processor
  property pattern : String = ""
  property replacement : String = ""
  property mode : String = "all"

  register "ReplaceText",
    description: "Regex find/replace on content",
    category: "Text",
    params: {
      pattern: {
        type: "Expression", required: true,
        placeholder: "\\berror\\b",
        description: "regex to match",
      },
      replacement: {type: "String", default: ""},
      mode: {
        type: "Enum", default: "all",
        choices: ["all", "first"],
      },
    }

  def process(ff : FlowFile) : Nil
    re = Regex.new(@pattern)
    out =
      case @mode
      when "first" then ff.text.sub(re, @replacement)
      else              ff.text.gsub(re, @replacement)
      end
    ff.text = out
    emit "success", ff
  end
end
