require "../processor"

# PutStdout — write FlowFile content (or its attrs / a one-line summary)
# to STDOUT. Handy terminal sink for demos + dev.
class PutStdout < Processor
  property format : String = "text"

  register "PutStdout",
    description: "Write FlowFile content to stdout",
    category: "Sink",
    params: {
      format: {
        type: "Enum", required: true, default: "text",
        choices: ["attrs", "raw", "text", "hex"],
      },
    }

  def process(ff : FlowFile) : Nil
    case @format
    when "attrs" then STDOUT.puts ff.attributes.to_json
    when "text"  then STDOUT.puts ff.text
    when "raw"
      STDOUT.write ff.content
      STDOUT.puts
    when "hex"   then STDOUT.puts ff.content.hexstring
    else raise "PutStdout: unknown format #{@format}"
    end
    emit "success", ff
  end
end
