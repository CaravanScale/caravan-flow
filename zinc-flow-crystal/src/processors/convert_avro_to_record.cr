require "avro"
require "json"
require "../processor"
require "../avro_schema_from_fields"

# ConvertAvroToRecord — decode FlowFile content as raw Avro binary
# (no OCF framing) using a schema built from the `fields` DSL. Stops
# reading at EOF or on a decode error; partial results are kept.
class ConvertAvroToRecord < Processor
  property fields : String = ""
  property schema_name : String = "default"

  register "ConvertAvroToRecord",
    description: "Decode Avro binary into records",
    category: "Conversion",
    wizard_component: "RecordFields",
    params: {
      fields: {
        type: "String", required: true,
        placeholder: "id:long,name:string,amount:double",
        description: "comma-separated name:type pairs describing the record schema",
      },
      schema_name: {type: "String", default: "default"},
    }

  def process(ff : FlowFile) : Nil
    schema = AvroSchemaFromFields.build(@schema_name, @fields)
    io = IO::Memory.new(ff.content)
    records = [] of Record

    loop do
      break if io.pos >= io.size
      avro_rec = Avro::BinaryCodec.read_record(io, schema)
      records << avro_record_to_flow_record(avro_rec)
    end

    ff.records = records
    ff.content_type = "records"
    ff.attributes["schema.name"] = @schema_name
    ff.attributes["record.count"] = records.size.to_s
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "avro decode error"
    emit "failure", ff
  end

  # Avro::Value → JSON::Any so it slots into FlowFile.records's shape.
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
