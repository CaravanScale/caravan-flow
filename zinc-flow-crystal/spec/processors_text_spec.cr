require "spec"
require "../src/processors/all"

describe ReplaceText do
  it "replaces all regex matches by default" do
    proc = Registry.create("ReplaceText", {"pattern" => "\\berror\\b", "replacement" => "ERR"})
    ff = FlowFile.new
    ff.text = "error here and error there"
    proc.process(ff)
    ff.text.should eq "ERR here and ERR there"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "honors mode=first" do
    proc = Registry.create("ReplaceText",
      {"pattern" => "a", "replacement" => "X", "mode" => "first"})
    ff = FlowFile.new
    ff.text = "aaa"
    proc.process(ff)
    ff.text.should eq "Xaa"
  end

  it "supports capture-group backrefs" do
    proc = Registry.create("ReplaceText",
      {"pattern" => "(\\d+)", "replacement" => "[\\1]"})
    ff = FlowFile.new
    ff.text = "code 42 and 7"
    proc.process(ff)
    ff.text.should eq "code [42] and [7]"
  end
end

describe ExtractText do
  it "extracts named capture groups into attributes" do
    proc = Registry.create("ExtractText",
      {"pattern" => "(?<user>\\w+)@(?<host>\\w+)"})
    ff = FlowFile.new
    ff.text = "alice@example"
    proc.process(ff)
    ff.attributes["user"].should eq "alice"
    ff.attributes["host"].should eq "example"
    proc.drain_outbox.first[0].should eq "success"
  end

  it "emits :unmatched when the pattern doesn't hit" do
    proc = Registry.create("ExtractText", {"pattern" => "\\d+"})
    ff = FlowFile.new
    ff.text = "no digits here"
    proc.process(ff)
    proc.drain_outbox.first[0].should eq "unmatched"
  end

  it "labels positional groups via group_names" do
    proc = Registry.create("ExtractText",
      {"pattern" => "(\\w+)-(\\w+)", "group_names" => "left,right"})
    ff = FlowFile.new
    ff.text = "foo-bar"
    proc.process(ff)
    ff.attributes["left"].should eq "foo"
    ff.attributes["right"].should eq "bar"
  end

  it "falls back to group.N when group_names is missing" do
    proc = Registry.create("ExtractText", {"pattern" => "(\\w+)"})
    ff = FlowFile.new
    ff.text = "hello"
    proc.process(ff)
    ff.attributes["group.1"].should eq "hello"
  end
end

describe SplitText do
  it "splits on newline by default, emitting one ff per chunk" do
    proc = Registry.create("SplitText", {"delimiter" => "\\n"})
    ff = FlowFile.new
    ff.text = "a\nb\nc"
    proc.process(ff)
    emitted = proc.drain_outbox
    emitted.size.should eq 3
    emitted.map { |(_, f)| f.text }.should eq ["a", "b", "c"]
    emitted.each { |(rel, _)| rel.should eq "success" }
  end

  it "tags each fragment with index + count attributes" do
    proc = Registry.create("SplitText", {"delimiter" => ","})
    ff = FlowFile.new
    ff.text = "x,y"
    proc.process(ff)
    emitted = proc.drain_outbox
    emitted[0][1].attributes["fragment.index"].should eq "0"
    emitted[0][1].attributes["fragment.count"].should eq "2"
    emitted[1][1].attributes["fragment.index"].should eq "1"
  end

  it "skips header_lines at the top before splitting" do
    proc = Registry.create("SplitText",
      {"delimiter" => "\\n", "header_lines" => "1"})
    ff = FlowFile.new
    ff.text = "HEADER\nrow1\nrow2"
    proc.process(ff)
    proc.drain_outbox.map { |(_, f)| f.text }.should eq ["row1", "row2"]
  end

  it "drops empty chunks" do
    proc = Registry.create("SplitText", {"delimiter" => ","})
    ff = FlowFile.new
    ff.text = "a,,b"
    proc.process(ff)
    proc.drain_outbox.size.should eq 2
  end
end
