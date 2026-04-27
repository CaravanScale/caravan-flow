# Does the `return out` bug cascade into "can't define def inside def"?
def a
  out = 5
  return out
end

def b
  42
end
