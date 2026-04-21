require "avro"

# Turns a caravan-flow `fields` DSL string ("id:long,name:string") into
# an Avro::Schema — shared by ConvertAvroToRecord / ConvertRecordToAvro
# / ConvertRecordToOCF so they all agree on the type mapping.
module AvroSchemaFromFields
  extend self

  TYPE_MAP = {
    "string"  => Avro::PrimitiveType::String,
    "bytes"   => Avro::PrimitiveType::Bytes,
    "int"     => Avro::PrimitiveType::Int,
    "long"    => Avro::PrimitiveType::Long,
    "float"   => Avro::PrimitiveType::Float,
    "double"  => Avro::PrimitiveType::Double,
    "boolean" => Avro::PrimitiveType::Boolean,
    "bool"    => Avro::PrimitiveType::Boolean,
    "null"    => Avro::PrimitiveType::Null,
  }

  def build(name : String, fields_dsl : String) : Avro::Schema
    fields = [] of Avro::Field
    fields_dsl.split(',').each do |raw|
      s = raw.strip
      next if s.empty?
      colon = s.index(':')
      if colon
        fname = s[0...colon].strip
        ftype_raw = s[(colon + 1)..].strip.downcase
        ftype = TYPE_MAP[ftype_raw]? || Avro::PrimitiveType::String
        fields << Avro::Field.new(fname, ftype)
      else
        fields << Avro::Field.new(s, Avro::PrimitiveType::String)
      end
    end
    raise "AvroSchemaFromFields: no fields parsed from '#{fields_dsl}'" if fields.empty?
    Avro::Schema.new(name, fields)
  end
end
