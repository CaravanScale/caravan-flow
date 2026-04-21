require "../processor"

# RouteOnAttribute — mini-DSL matching the C# sibling:
#   name: attr OP value; name2: attr2 OP2 value2
# Operators: EQ, NEQ, CONTAINS, STARTSWITH, ENDSWITH, EXISTS, GT, LT
# Each rule that matches emits the flowfile to its named relationship.
# Non-matching flowfiles go to the `unmatched` relationship.
class RouteOnAttribute < Processor
  property routes : String = ""

  register "RouteOnAttribute",
    description: "Route FlowFiles based on attribute predicates",
    category: "Routing",
    params: {
      routes: {
        type:        "String",
        required:    true,
        placeholder: "premium: tier EQ premium; bulk: tier EQ bulk",
        description: "semicolon-delimited `name: attr OP value` entries",
      },
    }

  record Rule, name : String, attr : String, op : String, value : String

  @parsed : Array(Rule)? = nil

  private def rules : Array(Rule)
    @parsed ||= parse(@routes)
  end

  private def parse(raw : String) : Array(Rule)
    out = [] of Rule
    raw.split(';').each do |chunk|
      s = chunk.strip
      next if s.empty?
      colon = s.index(':')
      next unless colon
      name = s[0...colon].strip
      rest = s[(colon + 1)..].strip.split(/\s+/, 3)
      next if rest.size < 2
      attr = rest[0]
      op = rest[1].upcase
      value = rest.size > 2 ? rest[2] : ""
      out << Rule.new(name, attr, op, value)
    end
    out
  end

  private def match?(rule : Rule, ff : FlowFile) : Bool
    av = ff.attributes[rule.attr]?
    case rule.op
    when "EXISTS"     then !av.nil?
    when "EQ"         then av == rule.value
    when "NEQ"        then av != rule.value
    when "CONTAINS"   then !!(av.try &.includes?(rule.value))
    when "STARTSWITH" then !!(av.try &.starts_with?(rule.value))
    when "ENDSWITH"   then !!(av.try &.ends_with?(rule.value))
    when "GT"         then (av.try(&.to_f?) || 0.0) > (rule.value.to_f? || 0.0)
    when "LT"         then (av.try(&.to_f?) || 0.0) < (rule.value.to_f? || 0.0)
    else false
    end
  end

  def process(ff : FlowFile) : Nil
    matched = false
    rules.each do |r|
      if match?(r, ff)
        emit r.name, ff
        matched = true
      end
    end
    emit "unmatched", ff unless matched
  end
end
