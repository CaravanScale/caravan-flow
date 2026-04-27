# `select` as an unqualified call inside a method body gets parsed as
# the opener of a fiber select/when/end statement, breaking code that
# wants a regular method call. Prepending `self.` or renaming to
# `evaluate` works around.

module Pipeline
  def self.select(a : Int32, b : Int32) : Int32
    a + b
  end

  def self.caller : Int32
    # This is the broken form: bare `select(...)` in call position.
    select(1, 2)
  end
end

puts Pipeline.caller
