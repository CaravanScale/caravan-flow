require "json"
require "./flowfile"

# ParamMeta is the JSON-facing schema per param. Optional fields
# (choices, entry_delim, pair_delim, value_kind) mirror zinc-csharp's
# ParamInfo — they're populated only for Enum / StringList / KeyValueList
# kinds so the UI can render the right widget.
struct ParamMeta
  include JSON::Serializable
  property name : String
  property type : String
  property required : Bool
  property placeholder : String?
  @[JSON::Field(key: "default")]
  property default_value : String?
  property description : String
  property choices : Array(String)?
  @[JSON::Field(key: "entryDelim")]
  property entry_delim : String?
  @[JSON::Field(key: "pairDelim")]
  property pair_delim : String?
  @[JSON::Field(key: "valueKind")]
  property value_kind : String?

  def initialize(
    @name,
    @type,
    @required,
    @placeholder = nil,
    @default_value = nil,
    @description = "",
    @choices = nil,
    @entry_delim = nil,
    @pair_delim = nil,
    @value_kind = nil,
  )
  end
end

struct ProcessorMeta
  include JSON::Serializable
  property name : String
  property description : String
  property category : String
  @[JSON::Field(key: "parameters")]
  property params : Array(ParamMeta)
  @[JSON::Field(key: "configKeys")]
  property config_keys : Array(String)
  property kind : String                  # "processor" | "source"
  @[JSON::Field(key: "wizardComponent", emit_null: true)]
  property wizard_component : String?

  def initialize(@name, @description, @category, @params, @kind = "processor", @wizard_component = nil)
    @config_keys = params.map(&.name)
  end
end

alias Factory = Proc(Hash(String, String), Processor)

module Registry
  @@factories = {} of String => Factory
  @@metas     = {} of String => ProcessorMeta

  def self.register_class(meta : ProcessorMeta, factory : Factory) : Nil
    @@factories[meta.name] = factory
    @@metas[meta.name] = meta
  end

  def self.create(name : String, config : Hash(String, String)) : Processor
    factory = @@factories[name]?
    raise "unknown processor type: #{name}" if factory.nil?
    factory.call(config)
  end

  def self.metas : Array(ProcessorMeta)
    @@metas.values
  end

  def self.known?(name : String) : Bool
    @@factories.has_key?(name)
  end
end
