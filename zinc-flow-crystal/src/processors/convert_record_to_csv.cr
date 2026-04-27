require "../processor"
require "csv"
require "json"

# ConvertRecordToCSV — serialize ff.records to CSV. Column order is
# pinned by the first record's key order (Crystal preserves insertion
# order in Hash), which matches what ConvertCSVToRecord produces.
class ConvertRecordToCSV < Processor
  property delimiter : String = ","
  property include_header : Bool = true

  register "ConvertRecordToCSV",
    description: "Serialize records to CSV",
    category: "Conversion",
    params: {
      delimiter: {type: "String", default: ","},
      include_header: {type: "Bool", default: "true"},
    }

  private def cell(v : JSON::Any) : String
    case raw = v.raw
    when Nil then ""
    else          raw.to_s
    end
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil? || rs.empty?
      emit "failure", ff
      return
    end
    columns = rs.first.keys
    io = IO::Memory.new
    CSV.build(io, separator: @delimiter[0]) do |csv|
      csv.row columns if @include_header
      rs.each do |rec|
        csv.row columns.map { |c| cell(rec[c]? || JSON::Any.new(nil)) }
      end
    end
    ff.text = io.to_s
    ff.attributes["mime.type"] = "text/csv"
    emit "success", ff
  end
end
