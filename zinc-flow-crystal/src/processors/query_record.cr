require "json"
require "jsonpath"
require "../processor"

# QueryRecord — filter a records batch with a JSONPath expression.
# Matching records pass through to :success; non-matching are dropped.
# Uses the sibling `jsonpath` shard.
#
# The query is compiled once on first use (cached on the processor),
# then applied per tick. Parse errors at compile time → :failure.
class QueryRecord < Processor
  property query : String = ""

  register "QueryRecord",
    description: "Filter records using a JsonPath query",
    category: "Record",
    wizard_component: "QueryRecord",
    params: {
      query: {
        type: "Expression", required: true,
        placeholder: "$[?(@.amount > 100)]",
        description: "JsonPath filter against the record batch",
      },
    }

  @compiled : JsonPath::Query? = nil

  private def compiled : JsonPath::Query
    @compiled ||= JsonPath.compile(@query)
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil?
      emit "failure", ff
      return
    end

    # Wrap records array as a JSON::Any so JsonPath can walk it.
    json_array = JSON::Any.new(rs.map { |r| JSON::Any.new(r) }.map(&.as(JSON::Any)))
    results = JsonPath.select(json_array, compiled)

    # Each result should be an object (a matched record). Fold back to
    # Array(Record) — JsonPath's nodelist semantics means we lost track
    # of which input index each came from, but that's fine for pass/drop.
    kept = [] of Record
    results.each do |r|
      if h = r.as_h?
        kept << h
      end
    end

    if kept.empty?
      ff.attributes["record.count"] = "0"
      emit "unmatched", ff
      return
    end

    ff.records = kept
    ff.attributes["record.count"] = kept.size.to_s
    emit "success", ff
  rescue e
    ff.attributes["error.message"] = e.message || "query error"
    emit "failure", ff
  end
end
