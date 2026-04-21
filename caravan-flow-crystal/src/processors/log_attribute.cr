require "../processor"

# LogAttribute — write the flowfile's attributes to stdout, prefixed with
# `prefix`. Useful for debugging the middle of a flow.
class LogAttribute < Processor
  property prefix : String = "flow"

  register "LogAttribute",
    description: "Logs FlowFile attributes and passes through",
    category: "Attribute",
    params: {
      prefix: {type: "String", default: "flow", description: "Log line prefix"},
    }

  def process(ff : FlowFile) : Nil
    STDOUT.puts "[#{@prefix}] #{ff.uuid} #{ff.attributes.inspect}"
    emit "success", ff
  end
end
