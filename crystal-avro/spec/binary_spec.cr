require "./spec_helper"

describe Avro::Encoding do
  it "zigzag encodes small signed ints to tight varints" do
    {0_i64, -1_i64, 1_i64, -2_i64, 2_i64, 63_i64, -64_i64, 64_i64}.each do |n|
      io = IO::Memory.new
      Avro::Encoding.write_varint(io, n)
      io.rewind
      Avro::Encoding.read_varint(io).should eq n
    end
  end

  it "round-trips Int64 extremes" do
    {Int64::MAX, Int64::MIN, 0_i64, 1234567890_i64, -1234567890_i64}.each do |n|
      io = IO::Memory.new
      Avro::Encoding.write_varint(io, n)
      io.rewind
      Avro::Encoding.read_varint(io).should eq n
    end
  end

  it "round-trips primitive codecs" do
    io = IO::Memory.new
    Avro::Encoding.write_boolean(io, true)
    Avro::Encoding.write_boolean(io, false)
    Avro::Encoding.write_int(io, 42)
    Avro::Encoding.write_long(io, 9_999_999_999_i64)
    Avro::Encoding.write_float(io, 3.14_f32)
    Avro::Encoding.write_double(io, -1.5e10)
    Avro::Encoding.write_string(io, "hello, avro")
    Avro::Encoding.write_bytes(io, Bytes[0x01, 0x02, 0x03])
    io.rewind

    Avro::Encoding.read_boolean(io).should be_true
    Avro::Encoding.read_boolean(io).should be_false
    Avro::Encoding.read_int(io).should eq 42
    Avro::Encoding.read_long(io).should eq 9_999_999_999_i64
    Avro::Encoding.read_float(io).should be_close(3.14_f32, 0.001_f32)
    Avro::Encoding.read_double(io).should be_close(-1.5e10, 0.001)
    Avro::Encoding.read_string(io).should eq "hello, avro"
    Avro::Encoding.read_bytes(io).should eq Bytes[0x01, 0x02, 0x03]
  end

  it "raises on truncated varint" do
    io = IO::Memory.new(Bytes[0x80])  # continuation bit set, no follow-up
    expect_raises(Avro::CodecError, /truncated/) do
      Avro::Encoding.read_varint(io)
    end
  end
end

describe Avro::BinaryCodec do
  it "round-trips a record with primitive fields" do
    schema = Avro::Schema.new("User", [
      Avro::Field.new("id",   Avro::PrimitiveType::Long),
      Avro::Field.new("name", Avro::PrimitiveType::String),
      Avro::Field.new("ok",   Avro::PrimitiveType::Boolean),
    ])

    record = {
      "id"   => 42_i64.as(Avro::Value),
      "name" => "zeus".as(Avro::Value),
      "ok"   => true.as(Avro::Value),
    } of String => Avro::Value

    io = IO::Memory.new
    Avro::BinaryCodec.write_record(io, record, schema)
    encoded = io.to_slice
    encoded.size.should be > 0

    io.rewind
    back = Avro::BinaryCodec.read_record(io, schema)
    back["id"].should eq 42_i64
    back["name"].should eq "zeus"
    back["ok"].should eq true
  end

  it "round-trips nullable fields (null + non-null branches)" do
    schema = Avro::Schema.new("Opt", [
      Avro::Field.new("email", Avro::PrimitiveType::String, nullable: true),
      Avro::Field.new("id",    Avro::PrimitiveType::Long,   nullable: true),
    ])

    samples = [] of Hash(String, Avro::Value)
    samples << ({"email" => nil.as(Avro::Value), "id" => 7_i64.as(Avro::Value)} of String => Avro::Value)
    samples << ({"email" => "x@y.com".as(Avro::Value), "id" => nil.as(Avro::Value)} of String => Avro::Value)
    samples << ({"email" => nil.as(Avro::Value), "id" => nil.as(Avro::Value)} of String => Avro::Value)

    samples.each do |r|
      io = IO::Memory.new
      Avro::BinaryCodec.write_record(io, r, schema)
      io.rewind
      back = Avro::BinaryCodec.read_record(io, schema)
      back["email"].should eq r["email"]
      back["id"].should eq r["id"]
    end
  end

  it "round-trips arrays and maps" do
    item = Avro::Field.new("item", Avro::PrimitiveType::String)
    vals = Avro::Field.new("val",  Avro::PrimitiveType::Int)

    schema = Avro::Schema.new("Bag", [
      Avro::Field.new("tags",  Avro::PrimitiveType::Array, element_type: item),
      Avro::Field.new("attrs", Avro::PrimitiveType::Map,   values_type: vals),
    ])

    tags_arr = ["a".as(Avro::Value), "bb".as(Avro::Value), "ccc".as(Avro::Value)] of Avro::Value
    attrs_map = {"x" => 1.as(Avro::Value), "y" => 2.as(Avro::Value)} of String => Avro::Value
    record = {
      "tags"  => tags_arr.as(Avro::Value),
      "attrs" => attrs_map.as(Avro::Value),
    } of String => Avro::Value

    io = IO::Memory.new
    Avro::BinaryCodec.write_record(io, record, schema)
    io.rewind
    back = Avro::BinaryCodec.read_record(io, schema)
    back["tags"].as(Array).should eq ["a", "bb", "ccc"]
    back["attrs"].as(Hash).should eq({"x" => 1, "y" => 2})
  end

  it "round-trips nested records" do
    inner = Avro::Schema.new("Customer", [
      Avro::Field.new("name", Avro::PrimitiveType::String),
    ])
    outer = Avro::Schema.new("Order", [
      Avro::Field.new("id",       Avro::PrimitiveType::Long),
      Avro::Field.new("customer", Avro::PrimitiveType::Record, record_schema: inner),
    ])

    inner_rec = {"name" => "alice".as(Avro::Value)} of String => Avro::Value
    record = {
      "id"       => 1_i64.as(Avro::Value),
      "customer" => inner_rec.as(Avro::Value),
    } of String => Avro::Value

    io = IO::Memory.new
    Avro::BinaryCodec.write_record(io, record, outer)
    io.rewind
    back = Avro::BinaryCodec.read_record(io, outer)
    back["id"].should eq 1_i64
    back["customer"].as(Avro::Record)["name"].should eq "alice"
  end
end
