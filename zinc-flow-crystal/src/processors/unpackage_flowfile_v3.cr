require "../processor"
require "../flowfile_v3"

# UnpackageFlowFileV3 — inverse of PackageFlowFileV3. Content is
# treated as one or more concatenated V3 frames; each frame becomes a
# fresh FlowFile with the embedded attributes restored.
class UnpackageFlowFileV3 < Processor
  register "UnpackageFlowFileV3",
    description: "Decode V3 binary content into one or more FlowFiles",
    category: "V3",
    params: {} of SymbolLiteral => NamedTupleLiteral

  def process(ff : FlowFile) : Nil
    frames = FlowFileV3.unpack_all(ff.content)
    if frames.empty?
      ff.attributes["error.message"] = "no valid V3 frames found"
      emit "failure", ff
      return
    end
    frames.each { |child| emit "success", child }
  end
end
