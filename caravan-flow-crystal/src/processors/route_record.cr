require "../processor"
require "../expression"

# RouteRecord — partition records across routes via per-route expression
# predicates. First-match wins; records that match none go to
# "unmatched". Each predicate sees the record's fields (via ident
# lookup) + the flowfile's attributes as a fallback.
class RouteRecord < Processor
  property routes : String = ""

  register "RouteRecord",
    description: "Partition records across routes via per-route expression predicates",
    category: "Routing",
    wizard_component: "RouteRecord",
    params: {
      routes: {
        type: "KeyValueList", required: true,
        value_kind: "Expression",
        entry_delim: ";", pair_delim: ":",
        placeholder: "premium: tier == \"gold\"; minors: age < 18",
        description: "name:expression pairs; first-match wins",
      },
    }

  @compiled : Array({String, Expr::Node})? = nil

  private def compiled : Array({String, Expr::Node})
    @compiled ||= (begin
      out = [] of {String, Expr::Node}
      @routes.split(';').each do |chunk|
        s = chunk.strip
        next if s.empty?
        colon = s.index(':')
        next unless colon
        out << {s[0...colon].strip, Expr::Parser.parse(s[(colon + 1)..].strip)}
      end
      out
    end)
  end

  private def truthy?(v : Expr::Value) : Bool
    case v
    when Nil    then false
    when Bool   then v
    when Int64  then v != 0_i64
    when Float64 then v != 0.0
    when String then !v.empty? && v != "false" && v != "0"
    end.as(Bool)
  end

  def process(ff : FlowFile) : Nil
    rs = ff.records
    if rs.nil?
      emit "failure", ff
      return
    end
    # Group records by matched route.
    buckets = Hash(String, Array(Record)).new { |h, k| h[k] = [] of Record }
    rs.each do |rec|
      ctx = Expr::Context.new(ff.attributes, rec)
      matched = false
      compiled.each do |name, node|
        begin
          if truthy?(node.eval(ctx))
            buckets[name] << rec
            matched = true
            break
          end
        rescue
          # A bad expression against a record: fall through, let it
          # land in unmatched rather than crashing the whole ff.
        end
      end
      buckets["unmatched"] << rec unless matched
    end
    buckets.each do |name, recs|
      next if recs.empty?
      child = ff.clone
      child.records = recs
      child.attributes["route"] = name
      emit name, child
    end
  end
end
