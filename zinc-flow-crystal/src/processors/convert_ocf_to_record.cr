require "avro"
require "json"
require "../processor"

# ConvertOCFToRecord — parse FlowFile content as an Avro OCF file.
# Writer's schema comes from the file's embedded metadata; decoded
# records populate ff.records. The content_type flips to "records" so
# downstream record processors pick up the typed batch.
class ConvertOCFToRecord < Processor
  register "ConvertOCFToRecord",
    description: "Decode Avro OCF (.avro file) into records",
    category: "Conversion",
    params: {} of SymbolLiteral => NamedTupleLiteral

  def process(ff : FlowFile) : Nil
    file = Avro::OCF.decode(ff.content)
    records = [] of Record
    file.records.each do |avro_rec|
      records << avro_record_to_flow_record(avro_rec)
    end
    ff.records = records
    ff.content_type = "records"
    ff.attributes["schema.name"] = file.schema.name
    ff.attributes["avro.codec"] = file.codec_name
    ff.attributes["record.count"] = records.size.to_s
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "OCF decode error"
    emit "failure", ff
  end

  private def avro_record_to_flow_record(ar : Avro::Record) : Record
    result = Record.new
    ar.each { |k, v| result[k] = to_json_any(v) }
    result
  end

  private def to_json_any(v : Avro::Value) : JSON::Any
    case v
    when Nil      then JSON::Any.new(nil)
    when Bool     then JSON::Any.new(v)
    when Int32    then JSON::Any.new(v.to_i64)
    when Int64    then JSON::Any.new(v)
    when Float32  then JSON::Any.new(v.to_f64)
    when Float64  then JSON::Any.new(v)
    when String   then JSON::Any.new(v)
    when Bytes    then JSON::Any.new(Base64.strict_encode(v))
    when Array    then JSON::Any.new(v.map { |x| to_json_any(x.as(Avro::Value)).as(JSON::Any) })
    when Hash
      h = {} of String => JSON::Any
      v.each { |k, x| h[k.as(String)] = to_json_any(x.as(Avro::Value)) }
      JSON::Any.new(h)
    else
      JSON::Any.new(v.to_s)
    end
  end
end

require "base64"
