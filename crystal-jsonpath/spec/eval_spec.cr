require "./spec_helper"

# Classic "store" document from the original JSONPath paper, lightly
# trimmed. Used by multiple cases below.
private STORE_JSON = <<-JSON
  {
    "store": {
      "book": [
        {"category": "reference", "author": "Nigel Rees",    "title": "Sayings of the Century", "price": 8.95},
        {"category": "fiction",   "author": "Evelyn Waugh",  "title": "Sword of Honour",        "price": 12.99},
        {"category": "fiction",   "author": "Herman Melville","title": "Moby Dick",             "price": 8.99, "isbn": "0-553-21311-3"},
        {"category": "fiction",   "author": "J. R. R. Tolkien","title": "The Lord of the Rings","price": 22.99, "isbn": "0-395-19395-8"}
      ],
      "bicycle": {"color": "red", "price": 19.95}
    },
    "threshold": 10.0
  }
  JSON

private def store : JSON::Any
  JSON.parse(STORE_JSON)
end

describe JsonPath::Eval do
  it "returns the whole document for the bare root query" do
    res = JsonPath.select(store, "$")
    res.size.should eq 1
    res.first.as_h.has_key?("store").should be_true
  end

  it "walks dotted member access" do
    res = JsonPath.select(store, "$.store.bicycle.color")
    res.map(&.as_s).should eq ["red"]
  end

  it "handles missing members by returning nothing" do
    JsonPath.select(store, "$.store.warehouse").should be_empty
  end

  it "picks an array element by positive index" do
    res = JsonPath.select(store, "$.store.book[0].author")
    res.map(&.as_s).should eq ["Nigel Rees"]
  end

  it "picks an array element by negative index" do
    res = JsonPath.select(store, "$.store.book[-1].author")
    res.map(&.as_s).should eq ["J. R. R. Tolkien"]
  end

  it "expands a wildcard over an array" do
    res = JsonPath.select(store, "$.store.book[*].title")
    res.map(&.as_s).should eq [
      "Sayings of the Century",
      "Sword of Honour",
      "Moby Dick",
      "The Lord of the Rings",
    ]
  end

  it "expands a wildcard over an object" do
    res = JsonPath.select(store, "$.store.bicycle.*")
    res.size.should eq 2
    res.map(&.raw).should contain "red"
  end

  it "slices an array with forward step" do
    res = JsonPath.select(store, "$.store.book[1:3].title")
    res.map(&.as_s).should eq ["Sword of Honour", "Moby Dick"]
  end

  it "slices an array with negative step" do
    res = JsonPath.select(store, "$.store.book[::-1].author")
    res.map(&.as_s).should eq [
      "J. R. R. Tolkien",
      "Herman Melville",
      "Evelyn Waugh",
      "Nigel Rees",
    ]
  end

  it "slices with negative start/end bounds" do
    res = JsonPath.select(store, "$.store.book[-2:].author")
    res.map(&.as_s).should eq ["Herman Melville", "J. R. R. Tolkien"]
  end

  it "filters with a numeric comparison" do
    res = JsonPath.select(store, "$.store.book[?(@.price < 10)].title")
    res.map(&.as_s).should eq ["Sayings of the Century", "Moby Dick"]
  end

  it "filters with a string equality" do
    res = JsonPath.select(store, %($.store.book[?(@.category == 'reference')].author))
    res.map(&.as_s).should eq ["Nigel Rees"]
  end

  it "filters with boolean AND + OR" do
    res = JsonPath.select(store,
      "$.store.book[?(@.category == 'fiction' && @.price < 15)].title")
    res.map(&.as_s).should eq ["Sword of Honour", "Moby Dick"]

    res2 = JsonPath.select(store,
      "$.store.book[?(@.price < 9 || @.price > 20)].title")
    res2.map(&.as_s).should eq ["Sayings of the Century", "Moby Dick", "The Lord of the Rings"]
  end

  it "filters with existence (bare reference)" do
    res = JsonPath.select(store, "$.store.book[?(@.isbn)].title")
    res.map(&.as_s).should eq ["Moby Dick", "The Lord of the Rings"]
  end

  it "filters with negation" do
    res = JsonPath.select(store, "$.store.book[?(!@.isbn)].title")
    res.map(&.as_s).should eq ["Sayings of the Century", "Sword of Honour"]
  end

  it "filters against a $-rooted reference" do
    res = JsonPath.select(store, "$.store.book[?(@.price < $.threshold)].title")
    res.map(&.as_s).should eq ["Sayings of the Century", "Moby Dick"]
  end

  it "treats type-mismatched comparisons as non-matches" do
    # "author" is a string; comparing to a number should match no rows
    # (no error raised — RFC 9535 semantics).
    res = JsonPath.select(store, "$.store.book[?(@.author < 10)]")
    res.should be_empty
  end

  it "compiles once and reuses across inputs" do
    q = JsonPath.compile("$.name")
    a = JsonPath.select(JSON.parse(%({"name": "alice"})), q)
    b = JsonPath.select(JSON.parse(%({"name": "bob"})), q)
    a.map(&.as_s).should eq ["alice"]
    b.map(&.as_s).should eq ["bob"]
  end

  it "exposes select_one convenience" do
    v = JsonPath.select_one(store, "$.store.bicycle.color")
    v.not_nil!.as_s.should eq "red"
    JsonPath.select_one(store, "$.nope").should be_nil
  end

  it "handles quoted names with dots, spaces, and escapes" do
    doc = JSON.parse(%({"weird key.with dots": {"x": 1}, "tab\\there": 2}))
    res = JsonPath.select(doc, %($['weird key.with dots'].x))
    res.map(&.raw).should eq [1_i64]
  end

  it "returns each wildcard expansion in document order" do
    doc = JSON.parse(%({"arr": [10, 20, 30, 40]}))
    JsonPath.select(doc, "$.arr[*]").map(&.raw).should eq [10_i64, 20_i64, 30_i64, 40_i64]
  end

  it "ignores index lookups on non-arrays" do
    doc = JSON.parse(%({"x": {"a": 1}}))
    JsonPath.select(doc, "$.x[0]").should be_empty
  end

  it "ignores name lookups on non-objects" do
    doc = JSON.parse(%({"x": [1, 2]}))
    JsonPath.select(doc, "$.x.nope").should be_empty
  end

  it "treats post-filter index against non-array nodes as non-matching" do
    # After `?(@.category == 'fiction')` the node-list contains book
    # *objects*, not a single array — so `[0]` applies to each object
    # individually and, because objects don't support numeric indexing,
    # returns nothing. This mirrors RFC 9535's nodelist model.
    res = JsonPath.select(store, "$.store.book[?(@.category == 'fiction')][0]")
    res.should be_empty
  end

  it "chains filter with a following member access across each match" do
    res = JsonPath.select(store, "$.store.book[?(@.category == 'fiction')].title")
    res.map(&.as_s).should eq ["Sword of Honour", "Moby Dick", "The Lord of the Rings"]
  end
end
