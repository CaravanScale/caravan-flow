require "./spec_helper"

# Minimal custom codec used to prove the plugin shape of Avro::Codecs.
# Wraps each block with a 2-byte "xx" prefix on compress, strips it on
# decompress. A real zstd / snappy codec has the same shape, just with
# a meaningful algorithm.
class PrefixTagCodec
  include Avro::Codec

  def name : String
    "xx-test"
  end

  def compress(bytes : Bytes) : Bytes
    io = IO::Memory.new
    io.write("xx".to_slice)
    io.write(bytes)
    io.to_slice
  end

  def decompress(bytes : Bytes) : Bytes
    bytes[2..]
  end
end

describe Avro::OCF do
  schema = Avro::SchemaJson.parse(<<-JSON)
    {
      "type": "record", "name": "User",
      "fields": [
        {"name": "id",    "type": "long"},
        {"name": "name",  "type": "string"},
        {"name": "email", "type": ["null", "string"]}
      ]
    }
    JSON

  make_records = ->{
    rs = [] of Avro::Record
    rs << ({
      "id" => 1_i64.as(Avro::Value),
      "name" => "alice".as(Avro::Value),
      "email" => "a@x.com".as(Avro::Value),
    } of String => Avro::Value)
    rs << ({
      "id" => 2_i64.as(Avro::Value),
      "name" => "bob".as(Avro::Value),
      "email" => nil.as(Avro::Value),
    } of String => Avro::Value)
    rs
  }

  it "round-trips with codec=null" do
    records = make_records.call
    bytes = Avro::OCF.encode(records, schema, codec_name: "null")
    file = Avro::OCF.decode(bytes)
    file.codec_name.should eq "null"
    file.records.size.should eq 2
    file.records[0]["name"].should eq "alice"
    file.records[1]["email"].should eq nil
  end

  it "round-trips with codec=deflate" do
    records = make_records.call
    # Pad the block enough that deflate actually saves bytes.
    big_records = (records * 200)
    raw = Avro::OCF.encode(big_records, schema, codec_name: "null")
    compressed = Avro::OCF.encode(big_records, schema, codec_name: "deflate")
    compressed.size.should be < raw.size

    file = Avro::OCF.decode(compressed)
    file.codec_name.should eq "deflate"
    file.records.size.should eq big_records.size
    file.records.first["name"].should eq "alice"
    file.records.last["email"].should eq nil
  end

  it "embeds the writer schema" do
    records = make_records.call
    bytes = Avro::OCF.encode(records, schema, codec_name: "null")
    file = Avro::OCF.decode(bytes)
    file.schema.name.should eq "User"
    file.schema.fields.map(&.name).should eq ["id", "name", "email"]
  end

  it "rejects bad magic" do
    bad = Bytes.new(100) { 0_u8 }
    expect_raises(Avro::OCF::InvalidFileError, /magic/) do
      Avro::OCF.decode(bad)
    end
  end

  it "rejects unknown codec names" do
    records = make_records.call
    expect_raises(Avro::CodecError, /no codec registered/) do
      Avro::OCF.encode(records, schema, codec_name: "snappy")
    end
  end

  it "lets callers register custom codecs (simulating zstd)" do
    # The PrefixTagCodec at the bottom of this file proves the plugin
    # shape — a real zstd adapter looks identical, just with meaningful
    # compression in compress/decompress.
    Avro::Codecs.register(PrefixTagCodec.new)
    Avro::Codecs.known?("xx-test").should be_true

    records = make_records.call
    bytes = Avro::OCF.encode(records, schema, codec_name: "xx-test")
    file = Avro::OCF.decode(bytes)
    file.codec_name.should eq "xx-test"
    file.records.size.should eq records.size
  end
end
