require "../processor"

# SplitRecord — fan out a FlowFile carrying N records into N FlowFiles,
# each with a single record. Attributes copied to every child.
class SplitRecord < Processor
  register "SplitRecord",
    description: "Fan out a records FlowFile into one FlowFile per record",
    category: "Record",
    params: {} of SymbolLiteral => NamedTupleLiteral

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil? || rs.empty?
      emit "failure", ff
      return
    end
    rs.each_with_index do |rec, idx|
      child = ff.clone
      child.records = [rec] of Record
      child.attributes["fragment.index"] = idx.to_s
      child.attributes["fragment.count"] = rs.size.to_s
      emit "success", child
    end
  end
end
