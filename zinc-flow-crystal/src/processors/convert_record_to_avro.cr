require "avro"
require "json"
require "../processor"
require "../avro_schema_from_fields"

# ConvertRecordToAvro — encode ff.records as raw Avro binary (no OCF
# framing) using a schema built from the `fields` DSL. Each record
# becomes a contiguous run of Avro-encoded fields.
class ConvertRecordToAvro < Processor
  property fields : String = ""
  property schema_name : String = "default"

  register "ConvertRecordToAvro",
    description: "Encode records to Avro binary",
    category: "Conversion",
    wizard_component: "RecordFields",
    params: {
      fields: {
        type: "String", required: true,
        placeholder: "id:long,name:string,amount:double",
        description: "comma-separated name:type pairs — must match the record shape",
      },
      schema_name: {type: "String", default: "default"},
    }

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil? || rs.empty?
      emit "failure", ff
      return
    end

    schema = AvroSchemaFromFields.build(@schema_name, @fields)
    io = IO::Memory.new
    rs.each do |rec|
      avro_rec = flow_record_to_avro(rec, schema)
      Avro::BinaryCodec.write_record(io, avro_rec, schema)
    end
    ff.content = io.to_slice
    ff.content_type = "bytes"
    ff.attributes["mime.type"] = "avro/binary"
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "avro encode error"
    emit "failure", ff
  end

  private def flow_record_to_avro(rec : Record, schema : Avro::Schema) : Avro::Record
    out_rec = {} of String => Avro::Value
    schema.fields.each do |f|
      out_rec[f.name] = coerce_for_field(rec[f.name]?, f)
    end
    out_rec
  end

  # The incoming FlowFile.records hold JSON::Any. Convert to the Avro
  # primitive the schema asks for. Coercion is narrow — raise on
  # mismatches rather than silently garble downstream.
  private def coerce_for_field(any : JSON::Any?, field : Avro::Field) : Avro::Value
    raw = any.try(&.raw)
    case field.type
    when Avro::PrimitiveType::Null    then nil
    when Avro::PrimitiveType::Boolean then raw.as(Bool)
    when Avro::PrimitiveType::Int     then int_of(raw).to_i32
    when Avro::PrimitiveType::Long    then int_of(raw)
    when Avro::PrimitiveType::Float   then float_of(raw).to_f32
    when Avro::PrimitiveType::Double  then float_of(raw)
    when Avro::PrimitiveType::String  then raw.as(String)
    when Avro::PrimitiveType::Bytes
      case raw
      when Bytes  then raw
      when String then raw.to_slice
      else raise "cannot coerce #{raw.class} to Bytes for field #{field.name}"
      end
    else raise "ConvertRecordToAvro: unsupported field type #{field.type} for #{field.name}"
    end
  end

  private def int_of(raw) : Int64
    case raw
    when Int64  then raw
    when Int32  then raw.to_i64
    when Float64 then raw.to_i64
    when Float32 then raw.to_i64
    when String then raw.to_i64? || 0_i64
    when Bool   then raw ? 1_i64 : 0_i64
    when Nil    then 0_i64
    else raise "cannot coerce #{raw.class} to Int64"
    end
  end

  private def float_of(raw) : Float64
    case raw
    when Float64 then raw
    when Float32 then raw.to_f64
    when Int64  then raw.to_f64
    when Int32  then raw.to_f64
    when String then raw.to_f64? || 0.0
    when Nil    then 0.0
    else raise "cannot coerce #{raw.class} to Float64"
    end
  end
end
