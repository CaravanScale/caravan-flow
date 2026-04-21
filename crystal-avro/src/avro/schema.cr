module Avro
  # Avro's primitive + complex types. Mirrors the type token in JSON
  # schemas (`"type": "record"`, `"int"`, etc.).
  enum PrimitiveType
    Null
    Boolean
    Int
    Long
    Float
    Double
    Bytes
    String
    Record
    Enum
    Array
    Map
    Union
    Fixed

    def to_avro_name : ::String
      case self
      when Null    then "null"
      when Boolean then "boolean"
      when Int     then "int"
      when Long    then "long"
      when Float   then "float"
      when Double  then "double"
      when Bytes   then "bytes"
      when String  then "string"
      when Record  then "record"
      when Enum    then "enum"
      when Array   then "array"
      when Map     then "map"
      when Union   then "union"
      when Fixed   then "fixed"
      else              "string"
      end
    end

    def self.from_avro_name(name : ::String) : PrimitiveType
      case name
      when "null"    then Null
      when "boolean" then Boolean
      when "int"     then Int
      when "long"    then Long
      when "float"   then Float
      when "double"  then Double
      when "bytes"   then Bytes
      when "string"  then String
      when "record"  then Record
      when "enum"    then Enum
      when "array"   then Array
      when "map"     then Map
      when "fixed"   then Fixed
      else raise InvalidSchemaError.new("unknown Avro type: #{name}")
      end
    end
  end

  # Avro logical types layered on top of a primitive. Each carries its
  # own metadata (precision/scale for Decimal). Unrecognized logical
  # names decode as `:none` — spec-compliant behavior per Avro 1.11 §2.
  enum LogicalType
    None
    Decimal
    Uuid
    Date
    TimeMillis
    TimeMicros
    TimestampMillis
    TimestampMicros

    def to_avro_name : ::String?
      case self
      when Decimal         then "decimal"
      when Uuid            then "uuid"
      when Date            then "date"
      when TimeMillis      then "time-millis"
      when TimeMicros      then "time-micros"
      when TimestampMillis then "timestamp-millis"
      when TimestampMicros then "timestamp-micros"
      else                      nil
      end
    end

    def self.from_avro_name(name : ::String) : LogicalType
      case name
      when "decimal"          then Decimal
      when "uuid"             then Uuid
      when "date"             then Date
      when "time-millis"      then TimeMillis
      when "time-micros"      then TimeMicros
      when "timestamp-millis" then TimestampMillis
      when "timestamp-micros" then TimestampMicros
      else                         None
      end
    end
  end

  class InvalidSchemaError < Exception; end

  class CodecError < Exception; end

  # A field within a record schema. `type` is the primitive; nested
  # records, arrays, maps, and unions carry their detail in
  # `element`, `values`, `branches`, and `record_schema`. `nullable`
  # is the common union-with-null shortcut (Avro's ["null", T] idiom).
  #
  # Logical-type modifiers ride alongside the primitive: `logical`,
  # plus `precision` / `scale` for Decimal. Aliases + default value
  # are preserved on round-trip.
  class Field
    property name : String
    property type : PrimitiveType
    property nullable : Bool
    property logical : LogicalType
    property precision : Int32
    property scale : Int32
    property record_schema : Schema?
    property element_type : Field?
    property values_type : Field?
    property union_branches : Array(PrimitiveType)
    property default_value : String?
    property doc : String?

    def initialize(
      @name : String,
      @type : PrimitiveType,
      *,
      @nullable = false,
      @logical = LogicalType::None,
      @precision = 0,
      @scale = 0,
      @record_schema = nil,
      @element_type = nil,
      @values_type = nil,
      @union_branches = [] of PrimitiveType,
      @default_value = nil,
      @doc = nil,
    )
    end
  end

  # A record schema: named, with an ordered field list. Records can
  # nest (a field of type Record carries its own Schema via
  # `record_schema`). The optional namespace matches Avro's dotted
  # FQN concept; `doc` is the human description.
  class Schema
    property name : String
    property namespace : String?
    property doc : String?
    property fields : Array(Field)
    property aliases : Array(String)

    def initialize(
      @name : String,
      @fields : Array(Field),
      *,
      @namespace = nil,
      @doc = nil,
      @aliases = [] of String,
    )
    end

    def fullname : String
      ns = @namespace
      ns && !ns.empty? ? "#{ns}.#{@name}" : @name
    end

    def field(name : String) : Field?
      @fields.find { |f| f.name == name }
    end
  end
end
