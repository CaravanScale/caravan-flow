require "json"

# Provenance ring buffer. Every CREATE / ROUTE / FAILURE event gets
# appended; the ring keeps the last N. Indexed by flowfile id for the
# lineage view. Module-level so processors + fabric can record without
# threading a logger through every call path.
module Provenance
  RING_CAPACITY = 2048

  struct Event
    include JSON::Serializable
    property timestamp : Int64
    property flowfile : String
    property type : String
    property component : String
    property details : String

    def initialize(@timestamp, @flowfile, @type, @component, @details = "")
    end
  end

  @@ring = Deque(Event).new
  @@mutex = Mutex.new

  def self.record(flowfile : String, type : String, component : String, details : String = "") : Nil
    ev = Event.new(Time.utc.to_unix_ms, flowfile, type, component, details)
    @@mutex.synchronize do
      @@ring.push(ev)
      while @@ring.size > RING_CAPACITY
        @@ring.shift
      end
    end
  end

  def self.recent(n : Int32) : Array(Event)
    @@mutex.synchronize do
      # Most-recent first.
      @@ring.to_a.reverse.first(n)
    end
  end

  def self.failures(n : Int32) : Array(Event)
    @@mutex.synchronize do
      @@ring.to_a.reverse.select { |e| e.type == "FAILURE" }.first(n)
    end
  end

  def self.by_id(flowfile : String) : Array(Event)
    @@mutex.synchronize do
      @@ring.to_a.select { |e| e.flowfile == flowfile || e.flowfile == "ff-#{flowfile}" || "ff-#{e.flowfile}" == flowfile }
    end
  end
end
