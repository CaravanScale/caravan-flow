require "../processor"
require "json"

# ConvertRecordToJSON — serialize ff.records back to JSON. Single record
# renders as an object; multiple render as an array.
class ConvertRecordToJSON < Processor
  register "ConvertRecordToJSON",
    description: "Serializes records back to JSON",
    category: "Conversion",
    params: {} of SymbolLiteral => NamedTupleLiteral

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil?
      emit "failure", ff
      return
    end
    payload = rs.size == 1 ? rs.first.to_json : rs.to_json
    ff.text = payload
    ff.attributes["mime.type"] = "application/json"
    emit "success", ff
  end
end
