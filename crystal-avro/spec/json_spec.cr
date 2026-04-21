require "./spec_helper"

describe Avro::SchemaJson do
  it "parses a record with primitive fields" do
    src = <<-JSON
      {
        "type": "record",
        "name": "User",
        "fields": [
          {"name": "id",   "type": "long"},
          {"name": "name", "type": "string"},
          {"name": "age",  "type": "int"}
        ]
      }
      JSON

    s = Avro::SchemaJson.parse(src)
    s.name.should eq "User"
    s.fields.size.should eq 3
    s.fields.map(&.name).should eq ["id", "name", "age"]
    s.fields.map(&.type).should eq [Avro::PrimitiveType::Long, Avro::PrimitiveType::String, Avro::PrimitiveType::Int]
    s.fields.all?(&.nullable).should be_false
  end

  it "lifts [null, T] union to nullable=true on the field" do
    src = <<-JSON
      {
        "type": "record",
        "name": "Opt",
        "fields": [
          {"name": "email", "type": ["null", "string"]},
          {"name": "phone", "type": ["string", "null"]}
        ]
      }
      JSON

    s = Avro::SchemaJson.parse(src)
    s.fields[0].nullable.should be_true
    s.fields[0].type.should eq Avro::PrimitiveType::String
    s.fields[1].nullable.should be_true
    s.fields[1].type.should eq Avro::PrimitiveType::String
  end

  it "extracts logical types" do
    src = <<-JSON
      {
        "type": "record",
        "name": "Event",
        "fields": [
          {"name": "ts",  "type": {"type": "long", "logicalType": "timestamp-millis"}},
          {"name": "amt", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}}
        ]
      }
      JSON

    s = Avro::SchemaJson.parse(src)
    s.fields[0].logical.should eq Avro::LogicalType::TimestampMillis
    s.fields[1].logical.should eq Avro::LogicalType::Decimal
    s.fields[1].precision.should eq 9
    s.fields[1].scale.should eq 2
  end

  it "parses nested records" do
    src = <<-JSON
      {
        "type": "record", "name": "Order",
        "fields": [
          {"name": "id", "type": "long"},
          {"name": "customer", "type": {
             "type": "record", "name": "Customer",
             "fields": [{"name": "name", "type": "string"}]
          }}
        ]
      }
      JSON

    s = Avro::SchemaJson.parse(src)
    cust = s.fields[1]
    cust.type.should eq Avro::PrimitiveType::Record
    cust.record_schema.should_not be_nil
    cust.record_schema.not_nil!.name.should eq "Customer"
    cust.record_schema.not_nil!.fields[0].name.should eq "name"
  end

  it "parses arrays and maps" do
    src = <<-JSON
      {
        "type": "record", "name": "Bag",
        "fields": [
          {"name": "tags",  "type": {"type": "array", "items": "string"}},
          {"name": "attrs", "type": {"type": "map",   "values": "int"}}
        ]
      }
      JSON

    s = Avro::SchemaJson.parse(src)
    tags = s.fields[0]
    tags.type.should eq Avro::PrimitiveType::Array
    tags.element_type.not_nil!.type.should eq Avro::PrimitiveType::String

    attrs = s.fields[1]
    attrs.type.should eq Avro::PrimitiveType::Map
    attrs.values_type.not_nil!.type.should eq Avro::PrimitiveType::Int
  end

  it "round-trips primitive record" do
    src = <<-JSON
      {"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}
      JSON

    schema = Avro::SchemaJson.parse(src)
    emitted = Avro::SchemaJson.emit(schema)
    # Re-parse — structural round-trip (not literal text, whitespace may differ)
    reparsed = Avro::SchemaJson.parse(emitted)
    reparsed.name.should eq schema.name
    reparsed.fields.map(&.name).should eq schema.fields.map(&.name)
    reparsed.fields.map(&.type).should eq schema.fields.map(&.type)
  end

  it "rejects a top-level non-record schema" do
    expect_raises(Avro::InvalidSchemaError, /must be a record/) do
      Avro::SchemaJson.parse(%({"type": "int"}))
    end
  end

  it "rejects a record without fields" do
    expect_raises(Avro::InvalidSchemaError, /missing 'fields'/) do
      Avro::SchemaJson.parse(%({"type": "record", "name": "x"}))
    end
  end
end
