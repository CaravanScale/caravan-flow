require "spec"
require "json"
require "../src/processors/all"

describe ConvertCSVToRecord do
  it "parses CSV with a header row into typed records" do
    proc = Registry.create("ConvertCSVToRecord",
      {"delimiter" => ",", "has_header" => "true"})
    ff = FlowFile.new
    ff.text = "id,name\n1,alice\n2,bob"
    proc.process(ff)
    ff.records.not_nil!.size.should eq 2
    ff.records.not_nil![0]["id"].as_s.should eq "1"
    ff.records.not_nil![0]["name"].as_s.should eq "alice"
    ff.records.not_nil![1]["name"].as_s.should eq "bob"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "honors a typed fields declaration" do
    proc = Registry.create("ConvertCSVToRecord",
      {"has_header" => "false", "fields" => "id:long,amount:double"})
    ff = FlowFile.new
    ff.text = "7,1.5"
    proc.process(ff)
    rec = ff.records.not_nil!.first
    rec["id"].as_i.should eq 7
    rec["amount"].as_f.should be_close(1.5, 0.001)
  end
end

describe ConvertRecordToCSV do
  it "round-trips records → CSV → records" do
    csv_in = Registry.create("ConvertCSVToRecord", {"has_header" => "true"})
    ff = FlowFile.new
    ff.text = "a,b\n1,x\n2,y"
    csv_in.process(ff)

    csv_out = Registry.create("ConvertRecordToCSV", {"include_header" => "true"})
    csv_out.process(ff)
    lines = ff.text.strip.split('\n')
    lines.first.should eq "a,b"
    lines.size.should eq 3
  end
end
