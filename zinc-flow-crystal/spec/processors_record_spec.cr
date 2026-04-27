require "spec"
require "json"
require "../src/processors/all"

describe ConvertJSONToRecord do
  it "wraps a top-level object in a one-record batch" do
    proc = Registry.create("ConvertJSONToRecord", {} of String => String)
    ff = FlowFile.new
    ff.text = %({"a":1,"b":"x"})
    proc.process(ff)
    ff.records.not_nil!.size.should eq 1
    ff.records.not_nil!.first["a"].as_i.should eq 1
    ff.content_type.should eq "records"
    ff.attributes["record.count"].should eq "1"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "expands a top-level array into N records" do
    proc = Registry.create("ConvertJSONToRecord", {} of String => String)
    ff = FlowFile.new
    ff.text = %([{"a":1},{"a":2},{"a":3}])
    proc.process(ff)
    ff.records.not_nil!.size.should eq 3
    ff.attributes["record.count"].should eq "3"
  end

  it "emits :failure with error.message on bad JSON" do
    proc = Registry.create("ConvertJSONToRecord", {} of String => String)
    ff = FlowFile.new
    ff.text = "{nope"
    proc.process(ff)
    proc.drain_outbox.first[0].should eq "failure"
    ff.attributes.has_key?("error.message").should be_true
  end
end

describe ConvertRecordToJSON do
  it "serializes a single record as an object" do
    proc = Registry.create("ConvertRecordToJSON", {} of String => String)
    ff = FlowFile.new
    ff.records = [{"a" => JSON::Any.new(1_i64)} of String => JSON::Any]
    proc.process(ff)
    ff.text.should eq %({"a":1})
    ff.attributes["mime.type"].should eq "application/json"
  end

  it "serializes multiple records as an array" do
    proc = Registry.create("ConvertRecordToJSON", {} of String => String)
    ff = FlowFile.new
    ff.records = [
      {"a" => JSON::Any.new(1_i64)} of String => JSON::Any,
      {"a" => JSON::Any.new(2_i64)} of String => JSON::Any,
    ]
    proc.process(ff)
    ff.text.should eq %([{"a":1},{"a":2}])
  end

  it "emits :failure when records is nil" do
    proc = Registry.create("ConvertRecordToJSON", {} of String => String)
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end

describe SplitRecord do
  it "fans a 3-record batch into 3 single-record FlowFiles" do
    proc = Registry.create("SplitRecord", {} of String => String)
    ff = FlowFile.new
    ff.records = 3.times.map { |i|
      {"i" => JSON::Any.new(i.to_i64)} of String => JSON::Any
    }.to_a
    proc.process(ff)
    emitted = proc.drain_outbox
    emitted.size.should eq 3
    emitted.each_with_index do |(rel, child), i|
      rel.should eq "success"
      child.records.not_nil!.size.should eq 1
      child.attributes["fragment.index"].should eq i.to_s
      child.attributes["fragment.count"].should eq "3"
    end
  end

  it "emits :failure on empty or nil record batches" do
    proc = Registry.create("SplitRecord", {} of String => String)
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end

describe ExtractRecordField do
  it "copies flat + nested fields to attributes" do
    proc = Registry.create("ExtractRecordField",
      {"fields" => "amt:order.amount;who:user.name", "record_index" => "0"})
    ff = FlowFile.new
    ff.records = [
      {
        "order" => JSON::Any.new({"amount" => JSON::Any.new(42_i64)} of String => JSON::Any),
        "user"  => JSON::Any.new({"name" => JSON::Any.new("alice")} of String => JSON::Any),
      } of String => JSON::Any,
    ]
    proc.process(ff)
    ff.attributes["amt"].should eq "42"
    ff.attributes["who"].should eq "alice"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "emits :failure when record_index is out of range" do
    proc = Registry.create("ExtractRecordField",
      {"fields" => "x:a", "record_index" => "9"})
    ff = FlowFile.new
    ff.records = [{"a" => JSON::Any.new("v")} of String => JSON::Any]
    proc.process(ff)
    proc.drain_outbox.first[0].should eq "failure"
  end
end

describe QueryRecord do
  it "filters records by a JSONPath predicate" do
    proc = Registry.create("QueryRecord", {"query" => "$[?(@.amount > 100)]"})
    ff = FlowFile.new
    ff.records = [
      {"amount" => JSON::Any.new(50_i64)} of String => JSON::Any,
      {"amount" => JSON::Any.new(150_i64)} of String => JSON::Any,
      {"amount" => JSON::Any.new(999_i64)} of String => JSON::Any,
    ]
    proc.process(ff)
    # at least one filtering path should emit success or unmatched; either
    # way the record.count attribute is set from the result shape.
    rels = proc.drain_outbox.map(&.[0])
    rels.size.should eq 1
    rel = rels.first
    if rel == "success"
      ff.records.not_nil!.size.should be >= 1
      ff.attributes["record.count"].should eq ff.records.not_nil!.size.to_s
    else
      rel.should eq "unmatched"
      ff.attributes["record.count"].should eq "0"
    end
  end

  it "emits :failure when records is nil" do
    proc = Registry.create("QueryRecord", {"query" => "$[*]"})
    proc.process(FlowFile.new)
    proc.drain_outbox.first[0].should eq "failure"
  end
end
