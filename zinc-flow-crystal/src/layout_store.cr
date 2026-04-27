require "json"

# In-memory map of node-name → {x, y} for the UI's drag-persisted
# positions. The zinc-csharp sibling writes this to a layout.yaml
# next to config.yaml; we do the simpler thing for the prototype and
# keep it in memory + a sibling file when a path is provided.
class LayoutStore
  struct Position
    include JSON::Serializable
    property x : Float64
    property y : Float64

    def initialize(@x, @y); end
  end

  @positions : Hash(String, Position)
  @mutex : Mutex
  @path : String?

  def initialize(@path : String? = nil)
    @positions = {} of String => Position
    @mutex = Mutex.new
    load_from_disk
  end

  def get : Hash(String, Position)
    @mutex.synchronize { @positions.dup }
  end

  def set(positions : Hash(String, Position)) : Nil
    @mutex.synchronize do
      @positions = positions
      save_to_disk
    end
  end

  def path : String?
    @path
  end

  private def load_from_disk : Nil
    p = @path
    return if p.nil? || !File.exists?(p)
    begin
      parsed = Hash(String, Position).from_json(File.read(p))
      @positions = parsed
    rescue
      # malformed layout file — fall back to empty; operator can re-save
    end
  end

  private def save_to_disk : Nil
    p = @path
    return if p.nil?
    File.write(p, @positions.to_json)
  end
end
