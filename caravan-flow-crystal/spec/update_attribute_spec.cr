require "spec"
require "../src/processors/update_attribute"

describe UpdateAttribute do
  it "applies key=value to a FlowFile and emits :success" do
    proc = Registry.create("UpdateAttribute", {"key" => "env", "value" => "prod"}).as(UpdateAttribute)
    ff = FlowFile.new
    proc.process(ff)

    ff.attributes["env"].should eq "prod"
    out = proc.drain_outbox
    out.size.should eq 1
    out.first[0].should eq "success"
  end

  it "raises on missing required params" do
    expect_raises(Exception, /missing required param: key/) do
      Registry.create("UpdateAttribute", {"value" => "prod"})
    end
  end

  it "exposes params via Registry.metas" do
    meta = Registry.metas.find! { |m| m.name == "UpdateAttribute" }
    meta.category.should eq "Attribute"
    meta.params.map(&.name).sort.should eq ["key", "value"]
    meta.params.all?(&.required).should be_true
  end
end
