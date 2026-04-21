require "../processor"

# GenerateFlowFile — timer-driven source. The fabric calls source_tick
# at a cadence driven by poll_interval_ms; we emit a FlowFile with the
# configured content and any attributes from the generator.
class GenerateFlowFile < Processor
  property content : String = "heartbeat"
  property batch_size : Int32 = 1

  register "GenerateFlowFile",
    description: "Timer-driven FlowFile generator for heartbeats + load tests",
    category: "Source",
    kind: "source",
    params: {
      content:    {type: "String", required: true, placeholder: "heartbeat"},
      batch_size: {type: "Int32", default: "1", description: "FlowFiles emitted per tick"},
    }

  # Sources never process inbound flowfiles — there aren't any.
  # Keeping the contract happy with a no-op.
  def process(ff : FlowFile) : Nil
  end

  def source_tick : Array(FlowFile)
    Array.new(@batch_size) do
      FlowFile.new(
        content: @content.to_slice,
        attributes: {"source" => "GenerateFlowFile"},
      )
    end
  end
end
