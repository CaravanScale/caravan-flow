# Per-edge counter: "from|rel|to" → processed count. Drives the
# caravan-in-motion animation on the canvas (UI polls /api/edge-stats).
# Keyed by string to match the UI's shape.
module EdgeStats
  @@counts = {} of String => Atomic(Int64)
  @@mutex = Mutex.new

  def self.key(from : String, rel : String, to : String) : String
    "#{from}|#{rel}|#{to}"
  end

  def self.bump(from : String, rel : String, to : String) : Nil
    k = key(from, rel, to)
    counter = @@mutex.synchronize do
      @@counts[k] ||= Atomic(Int64).new(0_i64)
    end
    counter.add(1_i64)
  end

  def self.snapshot : Hash(String, NamedTuple(processed: Int64))
    result = {} of String => NamedTuple(processed: Int64)
    @@mutex.synchronize do
      @@counts.each { |k, v| result[k] = {processed: v.get} }
    end
    result
  end

  def self.clear : Nil
    @@mutex.synchronize { @@counts.clear }
  end
end
