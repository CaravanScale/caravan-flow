# Per-edge counter: "from|rel|to" → processed count. Drives the
# zinc-in-motion animation on the canvas (UI polls /api/edge-stats).
# Keyed by string to match the UI's shape.
#
# Implementation note: we store plain Int64s under a mutex rather than
# Atomic(Int64). Atomic is a struct, and `Hash#[] ||=` plus a local
# rebind would return a copy — mutations on the local never reach the
# stored atomic. The mutex path is both simpler and actually correct.
module EdgeStats
  @@counts = {} of String => Int64
  @@mutex = Mutex.new

  def self.key(from : String, rel : String, to : String) : String
    "#{from}|#{rel}|#{to}"
  end

  def self.bump(from : String, rel : String, to : String) : Nil
    k = key(from, rel, to)
    @@mutex.synchronize do
      @@counts[k] = (@@counts[k]? || 0_i64) + 1_i64
    end
  end

  def self.snapshot : Hash(String, NamedTuple(processed: Int64))
    result = {} of String => NamedTuple(processed: Int64)
    @@mutex.synchronize do
      @@counts.each { |k, v| result[k] = {processed: v} }
    end
    result
  end

  def self.clear : Nil
    @@mutex.synchronize { @@counts.clear }
  end
end
