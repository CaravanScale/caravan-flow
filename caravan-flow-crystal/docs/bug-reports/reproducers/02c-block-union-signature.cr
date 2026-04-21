def yielder(&block : (Float64 | String), (Float64 | String) -> Bool)
  yield(1.0, 2.0)
end

yielder do |a, b|
  a == b
end
