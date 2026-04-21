require "../processor"
require "../flowfile_v3"

# PackageFlowFileV3 — fold the FlowFile's attributes + content into a
# single V3-binary blob. Typically chained before a transport sink
# (PutHTTP, PutFile) so attributes survive the wire.
class PackageFlowFileV3 < Processor
  register "PackageFlowFileV3",
    description: "Wrap (attributes + content) into NiFi V3 binary content",
    category: "V3",
    params: {} of SymbolLiteral => NamedTupleLiteral

  def process(ff : FlowFile) : Nil
    packed = FlowFileV3.pack(ff)
    ff.content = packed
    ff.content_type = "bytes"
    ff.attributes["mime.type"] = "application/flowfile-v3"
    emit "success", ff
  end
end
