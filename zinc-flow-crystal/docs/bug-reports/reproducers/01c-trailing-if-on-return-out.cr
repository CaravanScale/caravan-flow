# "return out" followed by trailing if (the FIRST bug we hit)
def a(cond : Bool); out = "x"; return out if cond; "y"; end
puts a(true)
puts a(false)
