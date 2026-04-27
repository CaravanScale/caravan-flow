require "../processor"
require "json"

# ConvertJSONToRecord — parse FlowFile content as JSON, materialize into
# records. A top-level object becomes one record; a top-level array
# becomes one FlowFile with N records.
class ConvertJSONToRecord < Processor
  property schema_name : String = "default"

  register "ConvertJSONToRecord",
    description: "Parses JSON content into records",
    category: "Conversion",
    params: {
      schema_name: {
        type: "String", default: "default",
        description: "schema name for the inferred record type",
      },
    }

  def process(ff : FlowFile) : Nil
    json = JSON.parse(ff.text)
    records =
      case json.raw
      when Array
        json.as_a.compact_map do |el|
          el.as_h? ? el.as_h : nil
        end.as(Array(Record))
      when Hash
        [json.as_h] of Record
      else
        raise "ConvertJSONToRecord: top-level JSON must be object or array, got #{json.raw.class}"
      end
    ff.records = records
    ff.content_type = "records"
    ff.attributes["schema.name"] = @schema_name
    ff.attributes["record.count"] = records.size.to_s
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "parse error"
    emit "failure", ff
  end
end
