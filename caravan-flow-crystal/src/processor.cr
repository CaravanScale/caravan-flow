require "./flowfile"
require "./registry"

# Base class every processor inherits. Concrete processors declare
# their config with the class-level `register` macro (see below) and
# implement `process`. Sources override `source_tick` to emit flowfiles
# on a timer. Record-oriented processors still use `process(ff)` —
# they just inspect / produce ff.records.
abstract class Processor
  @outbox : Array({String, FlowFile}) = [] of {String, FlowFile}

  abstract def process(ff : FlowFile) : Nil

  # Relationship is a String (not Symbol) — route names are dynamic
  # (RouteOnAttribute / RouteRecord) and Crystal doesn't allow dynamic
  # Symbol creation. Literal callers pay no runtime cost.
  def emit(rel : String, ff : FlowFile) : Nil
    @outbox << {rel, ff}
  end

  def drain_outbox : Array({String, FlowFile})
    result = @outbox
    @outbox = [] of {String, FlowFile}
    result
  end

  # Sources override this; returning a non-empty array emits flowfiles
  # on each fabric tick. Non-source processors return [].
  def source_tick : Array(FlowFile)
    [] of FlowFile
  end

  # Hook called by Fabric on add/hot-reload for sources that hold OS
  # resources (file watchers, sockets). Default no-op.
  def on_start : Nil
  end

  def on_stop : Nil
  end

  # Class-level macro — each concrete processor calls this once inside
  # its body to declare metadata + config binding. Macro emits:
  #   - ProcessorMeta record (for /api/registry)
  #   - Factory closure (config hash → initialized instance, type-coerced)
  #   - Top-level Registry.register_class call at load time
  # Result: one source of truth per param, no reflection.
  macro register(
    name,
    description = "",
    category = "Other",
    params = {} of SymbolLiteral => NamedTupleLiteral,
    kind = "processor",
    wizard_component = nil,
  )
    ::Registry.register_class(
      ::ProcessorMeta.new(
        name: {{name}},
        description: {{description}},
        category: {{category}},
        kind: {{kind}},
        wizard_component: {% if wizard_component %}{{wizard_component}}{% else %}nil{% end %},
        params: [
          {% for pname, pspec in params %}
            ::ParamMeta.new(
              name: {{pname.id.stringify}},
              type: {{pspec[:type] || "String"}},
              required: {{pspec[:required] == true}},
              placeholder: {% if pspec[:placeholder] %}{{pspec[:placeholder]}}{% else %}nil.as(String?){% end %},
              default_value: {% if pspec[:default] %}{{pspec[:default]}}{% else %}nil.as(String?){% end %},
              description: {{pspec[:description] || ""}},
              choices: {% if pspec[:choices] %}{{pspec[:choices]}} of String{% else %}nil.as(Array(String)?){% end %},
              entry_delim: {% if pspec[:entry_delim] %}{{pspec[:entry_delim]}}{% else %}nil.as(String?){% end %},
              pair_delim: {% if pspec[:pair_delim] %}{{pspec[:pair_delim]}}{% else %}nil.as(String?){% end %},
              value_kind: {% if pspec[:value_kind] %}{{pspec[:value_kind]}}{% else %}nil.as(String?){% end %},
            ),
          {% end %}
        ] of ::ParamMeta,
      ),
      ->(cfg : Hash(String, String)) {
        inst = {{@type}}.new
        {% for pname, pspec in params %}
          raw = cfg[{{pname.id.stringify}}]?
          if raw.nil?
            {% if pspec[:required] == true %}
              raise "missing required param: " + {{pname.id.stringify}}
            {% else %}
              raw = {{pspec[:default] || ""}}
            {% end %}
          end
          {% t = pspec[:type] || "String" %}
          {% if t == "Int32" || t == "Integer" %}
            inst.{{pname.id}} = raw.not_nil!.to_i
          {% elsif t == "Int64" %}
            inst.{{pname.id}} = raw.not_nil!.to_i64
          {% elsif t == "Bool" || t == "Boolean" %}
            inst.{{pname.id}} = (raw.not_nil! == "true")
          {% elsif t == "Float64" %}
            inst.{{pname.id}} = raw.not_nil!.to_f
          {% else %}
            inst.{{pname.id}} = raw.not_nil!
          {% end %}
        {% end %}
        inst.on_start
        inst.as(::Processor)
      }
    )
  end
end
