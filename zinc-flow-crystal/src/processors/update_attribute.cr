require "../processor"

# UpdateAttribute — sets or overwrites a single key=value pair on every
# FlowFile passing through. The whole processor is ~10 lines thanks to
# `register`: `property` lines are the typed binding, the macro emits
# the factory + metadata from the same hash.
class UpdateAttribute < Processor
  property key : String = ""
  property value : String = ""

  register "UpdateAttribute",
    description: "Sets key=value attribute on FlowFiles",
    category: "Attribute",
    params: {
      key:   {type: "String", required: true, placeholder: "env"},
      value: {type: "String", required: true, placeholder: "prod"},
    }

  def process(ff : FlowFile) : Nil
    ff.attributes[@key] = @value
    emit "success", ff
  end
end
