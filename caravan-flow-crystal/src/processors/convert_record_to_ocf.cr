require "avro"
require "json"
require "../processor"
require "../avro_schema_from_fields"

# ConvertRecordToOCF — encode ff.records into an Avro Object Container
# File with the chosen block codec. `fields` drives the embedded
# schema; the compressor codec is one of {null, deflate, zstandard}.
# Zstd is wired through crystal-avro's plugin interface in
# avro_zstd_adapter.cr.
class ConvertRecordToOCF < Processor
  property codec : String = "null"
  property fields : String = ""
  property schema_name : String = "default"

  register "ConvertRecordToOCF",
    description: "Encode records to Avro OCF (.avro file)",
    category: "Conversion",
    wizard_component: "RecordFields",
    params: {
      codec: {
        type: "Enum", required: true, default: "null",
        choices: ["null", "deflate", "zstandard"],
      },
      fields: {
        type: "String", required: true,
        placeholder: "id:long,name:string,amount:double",
        description: "schema to embed in the OCF metadata",
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
    avro_records = rs.map { |r| flow_record_to_avro(r, schema).as(Avro::Record) }
    bytes = Avro::OCF.encode(avro_records, schema, codec_name: @codec)
    ff.content = bytes
    ff.content_type = "bytes"
    ff.attributes["mime.type"] = "avro/binary"
    ff.attributes["avro.codec"] = @codec
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "OCF encode error"
    emit "failure", ff
  end

  private def flow_record_to_avro(rec : Record, schema : Avro::Schema) : Avro::Record
    out_rec = {} of String => Avro::Value
    schema.fields.each do |f|
      out_rec[f.name] = coerce_for_field(rec[f.name]?, f)
    end
    out_rec
  end

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
    else raise "ConvertRecordToOCF: unsupported field type #{field.type} for #{field.name}"
    end
  end

  private def int_of(raw) : Int64
    case raw
    when Int64   then raw
    when Int32   then raw.to_i64
    when Float64 then raw.to_i64
    when Float32 then raw.to_i64
    when String  then raw.to_i64? || 0_i64
    when Bool    then raw ? 1_i64 : 0_i64
    when Nil     then 0_i64
    else raise "cannot coerce #{raw.class} to Int64"
    end
  end

  private def float_of(raw) : Float64
    case raw
    when Float64 then raw
    when Float32 then raw.to_f64
    when Int64   then raw.to_f64
    when Int32   then raw.to_f64
    when String  then raw.to_f64? || 0.0
    when Nil     then 0.0
    else raise "cannot coerce #{raw.class} to Float64"
    end
  end
end
