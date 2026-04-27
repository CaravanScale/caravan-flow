package tests

import (
	"testing"
	"zinc-flow/processors"
	"zinc-flow/core"
	"github.com/ZincScale/zinc-stdlib/asserts"
	"strings"
	"zinc-flow/fabric/source"
	"fmt"
)

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:11
func TestAddAttributeSetsAttributePreservesExistingAttrs(t *testing.T) {
	proc := processors.NewAddAttribute("env", "prod")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"type": "order"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["env"], "prod")
		asserts.EqualString(t, out.Attributes["type"], "order")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:25
func TestLogProcessorPassesThroughUnchanged(t *testing.T) {
	proc := processors.NewLogProcessor("test")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"type": "order"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.IsTrue(t, out.Id == ff.Id, "id preserved")
		asserts.EqualString(t, out.Attributes["type"], "order")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:39
func TestFileSinkWritesToDiskReturnsDropped(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewFileSink("/tmp/zinc-flow-test/output", store)
	ff := core.CreateFlowFile([]byte("sink test"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Dropped:
		reason := _v.Reason
		asserts.IsTrue(t, strings.HasPrefix(reason, "written to"), "reason starts with 'written to'")
	default:
		_ = _v
		t.Errorf("expected Dropped, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:53
func TestJsonToRecordsParsesJSONArrayIntoRecords(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewJsonToRecords("order", store)
	ff := core.CreateFlowFile(JsonArrayFixture(), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.IsTrue(t, core.IsRecord(out.Content), "content is records")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:67
func TestRecordsToJsonSerializesRecordsBackToJSONBytes(t *testing.T) {
	fields := []core.Field{core.NewField("name", core.STRING, nil)}
	schema := core.SchemaFromFields("test", fields)
	rec := core.NewGenericRecord(schema)
	rec.SetField("name", "Bob")
	ff := core.CreateFlowFileWithContent(core.NewRecords(schema, []*core.GenericRecord{rec}), map[string]string{})
	proc := processors.NewRecordsToJson()
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.IsFalse(t, core.IsRecord(out.Content), "output is raw not records")
		store := core.NewMemoryContentStore()
		resolved := core.Resolve(store, out.Content)
		asserts.Contains(t, string(resolved.Data), "Bob")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:92
func TestJSONRoundtripRecordsJSONRecordsPreservesData(t *testing.T) {
	fields := []core.Field{core.NewField("city", core.STRING, nil)}
	schema := core.SchemaFromFields("geo", fields)
	rec := core.NewGenericRecord(schema)
	rec.SetField("city", "Portland")
	ff := core.CreateFlowFileWithContent(core.NewRecords(schema, []*core.GenericRecord{rec}), map[string]string{})
	store := core.NewMemoryContentStore()
	toJson := processors.NewRecordsToJson()
	step1 := toJson.Process(ff)
	switch _v := step1.(type) {
	case core.Single:
		jsonFF := _v.Ff
		asserts.IsFalse(t, core.IsRecord(jsonFF.Content), "step1 output is raw")
		resolved := core.Resolve(store, jsonFF.Content)
		asserts.Contains(t, string(resolved.Data), "Portland")
		toRec := processors.NewJsonToRecords("geo", store)
		step2 := toRec.Process(jsonFF)
		switch _v := step2.(type) {
		case core.Single:
			recordsFF := _v.Ff
			asserts.IsTrue(t, core.IsRecord(recordsFF.Content), "step2 output is records")
		default:
			_ = _v
			t.Errorf("step2: expected Single, got %v", step2)
		}
	default:
		_ = _v
		t.Errorf("step1: expected Single, got %v", step1)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:126
func TestJsonToRecordsEmptyJSONArrayReturnsFailure(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewJsonToRecords("test", store)
	ff := core.CreateFlowFile([]byte("[]"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Failure:
		reason := _v.Reason
		asserts.Contains(t, reason, "no records")
	default:
		_ = _v
		t.Errorf("expected Failure, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:151
func TestUpdateAttributeSetsAttributeAndPreservesExisting(t *testing.T) {
	proc := processors.NewUpdateAttribute("env", "prod")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"type": "order"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["env"], "prod")
		asserts.EqualString(t, out.Attributes["type"], "order")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:165
func TestUpdateAttributeOverwritesExistingAttributeValue(t *testing.T) {
	proc := processors.NewUpdateAttribute("env", "prod")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"env": "dev"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["env"], "prod")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:178
func TestUpdateAttributeEmptyKeyValueValuesStillProduceSingleNoPanic(t *testing.T) {
	proc := processors.NewUpdateAttribute("", "")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		_ = _v
	default:
		_ = _v
		t.Errorf("expected Single even for empty config, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:191
func TestRouteOnAttributeMatchesFirstPredicateRoutesToItsRelationship(t *testing.T) {
	proc := processors.NewRouteOnAttribute("high: priority EQ high; low: priority EQ low")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"priority": "high"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Routed:
		route := _v.Route
		out := _v.Ff
		asserts.EqualString(t, route, "high")
		asserts.EqualString(t, out.Attributes["priority"], "high")
	default:
		_ = _v
		t.Errorf("expected Routed, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:205
func TestRouteOnAttributeFallsThroughToUnmatchedWhenNoPredicateMatches(t *testing.T) {
	proc := processors.NewRouteOnAttribute("us: region EQ us; eu: region EQ eu")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{"region": "apac"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "unmatched")
	default:
		_ = _v
		t.Errorf("expected Routed(unmatched), got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:218
func TestRouteOnAttributeMissingAttributeOnNonEXISTSPredicateUnmatched(t *testing.T) {
	proc := processors.NewRouteOnAttribute("ok: missing EQ yes")
	ff := core.CreateFlowFile([]byte("data"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "unmatched")
	default:
		_ = _v
		t.Errorf("expected Routed(unmatched), got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:233
func TestRouteOnAttributeCONTAINSSTARTSWITHEXISTSOperatorsDispatchCorrectly(t *testing.T) {
	proc := processors.NewRouteOnAttribute("has_path: url CONTAINS /api; proto_http: url STARTSWITH http; any: url EXISTS")
	ff1 := core.CreateFlowFile([]byte("1"), map[string]string{"url": "https://host/api/v1"})
	switch _v := proc.Process(ff1).(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "has_path")
	default:
		_ = _v
		t.Errorf("shape1 expected Routed")
	}
	ff2 := core.CreateFlowFile([]byte("2"), map[string]string{"url": "http://example.com"})
	switch _v := proc.Process(ff2).(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "proto_http")
	default:
		_ = _v
		t.Errorf("shape2 expected Routed")
	}
	ff3 := core.CreateFlowFile([]byte("3"), map[string]string{"url": "ftp://x"})
	switch _v := proc.Process(ff3).(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "any")
	default:
		_ = _v
		t.Errorf("shape3 expected Routed")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:252
func TestRouteOnAttributeEmptyConfigRoutesEverythingToUnmatched(t *testing.T) {
	proc := processors.NewRouteOnAttribute("")
	ff := core.CreateFlowFile([]byte("x"), map[string]string{"a": "b"})
	switch _v := proc.Process(ff).(type) {
	case core.Routed:
		route := _v.Route
		asserts.EqualString(t, route, "unmatched")
	default:
		_ = _v
		t.Errorf("expected Routed(unmatched) for empty routes")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:261
func TestPutStdoutAttrsFormatPassesThroughUnchangedAsSingle(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewPutStdout("attrs", store)
	ff := core.CreateFlowFile([]byte("hello"), map[string]string{"k": "v"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Id, ff.Id)
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:273
func TestPutStdoutRawFormatWithRawContentPassesThrough(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewPutStdout("raw", store)
	ff := core.CreateFlowFile([]byte("payload"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		_ = _v
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:287
func TestGenerateFlowFilePollOnceEmitsBatchSizeFlowFilesWithBaseAttrs(t *testing.T) {
	src := source.NewGenerateFlowFile("gen-test", 0, "hello", "", "env:dev;region:us", 3)
	batch := src.PollOnce()
	asserts.EqualInt(t, len(batch), 3)
	for _, ff := range batch {
		asserts.EqualString(t, string(core.Resolve(core.NewMemoryContentStore(), ff.Content).Data), "hello")
		asserts.EqualString(t, ff.Attributes["env"], "dev")
		asserts.EqualString(t, ff.Attributes["region"], "us")
		asserts.EqualString(t, ff.Attributes["source"], "gen-test")
		asserts.IsTrue(t, ff.Attributes["generate.index"] != "", "generate.index set")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:300
func TestGenerateFlowFileGenerateIndexIncrementsAcrossPollCycles(t *testing.T) {
	src := source.NewGenerateFlowFile("gen-counter", 0, "", "", "", 1)
	first := src.PollOnce()
	second := src.PollOnce()
	asserts.EqualInt(t, len(first), 1)
	asserts.EqualInt(t, len(second), 1)
	asserts.IsTrue(t, first[0].Attributes["generate.index"] != second[0].Attributes["generate.index"], "index advances")
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:309
func TestGenerateFlowFileMalformedAttributesSpecToleratedNoPanic(t *testing.T) {
	src := source.NewGenerateFlowFile("gen-malformed", 0, "x", "", ";;broken;:empty-key;onlykey;", 1)
	batch := src.PollOnce()
	asserts.EqualInt(t, len(batch), 1)
	asserts.EqualString(t, batch[0].Attributes["source"], "gen-malformed")
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:321
func buildRecordFlowFile(kv map[string]interface{}) core.FlowFile {
	fields := []core.Field{}
	for _, k := range func() []string { _keys := make([]string, 0, len(kv)); for _k := range kv { _keys = append(_keys, _k) }; return _keys }() {
		fields = append(fields, core.NewField(k, core.STRING, nil))
	}
	schema := core.NewSchema("test", fields)
	rec := core.NewGenericRecord(schema)
	for _, k := range func() []string { _keys := make([]string, 0, len(kv)); for _k := range kv { _keys = append(_keys, _k) }; return _keys }() {
		rec.SetField(k, kv[k])
	}
	records := []*core.GenericRecord{rec}
	return core.NewFlowFile("ff-test", map[string]string{}, core.NewRecords(schema, records), 0)
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:335
func TestExtractRecordFieldExtractsFieldsOntoAttributes(t *testing.T) {
	proc := processors.NewExtractRecordField("name:customer.name;amount:order.total", 0)
	ff := buildRecordFlowFile(map[string]interface{}{"name": "alice", "amount": "42"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["customer.name"], "alice")
		asserts.EqualString(t, out.Attributes["order.total"], "42")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:349
func TestExtractRecordFieldMissingFieldSilentlySkippedNotAFailure(t *testing.T) {
	proc := processors.NewExtractRecordField("name:cname;absent:ghost", 0)
	ff := buildRecordFlowFile(map[string]interface{}{"name": "bob"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["cname"], "bob")
		asserts.IsFalse(t, func() bool { _, _ok := out.Attributes["ghost"]; return _ok }(), "ghost attribute not set")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:363
func TestExtractRecordFieldNonRecordsContentPassesThroughUnchanged(t *testing.T) {
	proc := processors.NewExtractRecordField("x:y", 0)
	ff := core.CreateFlowFile([]byte("raw"), map[string]string{"orig": "attr"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["orig"], "attr")
		asserts.IsFalse(t, func() bool { _, _ok := out.Attributes["y"]; return _ok }(), "no extraction on Raw content")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:377
func TestExtractRecordFieldMalformedConfigSkippedEmptyExtractsNoAttrs(t *testing.T) {
	proc := processors.NewExtractRecordField("garbage;no_colon;:empty_field;name_only:;", 0)
	ff := buildRecordFlowFile(map[string]interface{}{"name": "x"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualInt(t, len(out.Attributes), 0)
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:389
func firstRecordFields(ff core.FlowFile) *core.GenericRecord {
	switch _v := ff.Content.(type) {
	case core.Records:
		records := _v.Rows
		return records[0]
	default:
		_ = _v
		return core.NewGenericRecord(core.NewSchema("empty", []core.Field{}))
	}
	return nil
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:401
func TestTransformRecordRenameAddRemoveOperationsApplyToEveryRecord(t *testing.T) {
	proc := processors.NewTransformRecord("rename:name:customer; add:env:dev; remove:obsolete")
	ff := buildRecordFlowFile(map[string]interface{}{"name": "alice", "obsolete": "drop"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		r := firstRecordFields(out)
		asserts.EqualString(t, fmt.Sprint(r.GetField("customer")), "alice")
		asserts.EqualString(t, fmt.Sprint(r.GetField("env")), "dev")
		asserts.IsTrue(t, r.GetField("name") == nil, "old name removed")
		asserts.IsTrue(t, r.GetField("obsolete") == nil, "obsolete removed")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:418
func TestTransformRecordCopyToUpperToLowerDefault(t *testing.T) {
	proc := processors.NewTransformRecord("copy:src:dst; toUpper:src; toLower:other; default:missing:DEFAULT")
	ff := buildRecordFlowFile(map[string]interface{}{"src": "hello", "other": "WORLD"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		r := firstRecordFields(out)
		asserts.EqualString(t, fmt.Sprint(r.GetField("src")), "HELLO")
		asserts.EqualString(t, fmt.Sprint(r.GetField("dst")), "hello")
		asserts.EqualString(t, fmt.Sprint(r.GetField("other")), "world")
		asserts.EqualString(t, fmt.Sprint(r.GetField("missing")), "DEFAULT")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:435
func TestTransformRecordUnknownOpInclComputeSkippedNoPanic(t *testing.T) {
	proc := processors.NewTransformRecord("compute:total:amount * qty; rename:x:y")
	ff := buildRecordFlowFile(map[string]interface{}{"x": "one", "amount": "2", "qty": "3"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		r := firstRecordFields(out)
		asserts.EqualString(t, fmt.Sprint(r.GetField("y")), "one")
		asserts.IsTrue(t, r.GetField("total") == nil, "compute op ignored")
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:453
func TestTransformRecordNonRecordsContentPassesThrough(t *testing.T) {
	proc := processors.NewTransformRecord("add:x:y")
	ff := core.CreateFlowFile([]byte("raw"), map[string]string{})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		_ = _v
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:464
func TestReplaceTextModeAllReplacesEveryOccurrence(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewReplaceText("foo", "bar", "all", store)
	ff := core.CreateFlowFile([]byte("foo and foo again"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		switch _v := out.Content.(type) {
		case core.Raw:
			bytes := _v.Bytes
			asserts.EqualString(t, string(bytes), "bar and bar again")
		default:
			_ = _v
			t.Errorf("expected Raw content")
		}
	default:
		_ = _v
		t.Errorf("expected Single, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:481
func TestReplaceTextModeFirstReplacesOnlyTheFirstMatch(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewReplaceText("foo", "bar", "first", store)
	ff := core.CreateFlowFile([]byte("foo and foo"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		switch _v := out.Content.(type) {
		case core.Raw:
			bytes := _v.Bytes
			asserts.EqualString(t, string(bytes), "bar and foo")
		default:
			_ = _v
			t.Errorf("expected Raw")
		}
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:498
func TestReplaceTextInvalidRegexPatternReturnsFailureNotPanic(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewReplaceText("[unterminated", "x", "all", store)
	ff := core.CreateFlowFile([]byte("test"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Failure:
		reason := _v.Reason
		asserts.Contains(t, reason, "invalid regex")
	default:
		_ = _v
		t.Errorf("expected Failure on bad regex, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:511
func TestExtractTextNamedCaptureGroupsBecomeAttributes(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewExtractText("(?P<proto>https?)://(?P<host>[^/]+)", "", store)
	ff := core.CreateFlowFile([]byte("visit https://example.com/path"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["proto"], "https")
		asserts.EqualString(t, out.Attributes["host"], "example.com")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:526
func TestExtractTextPositionalGroupsViaGroupNamesConfig(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewExtractText("(\\d+)-(\\d+)", "major,minor", store)
	ff := core.CreateFlowFile([]byte("version 42-7 released"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["major"], "42")
		asserts.EqualString(t, out.Attributes["minor"], "7")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:541
func TestExtractTextNoMatchReturnsSingleWithUnchangedAttrs(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewExtractText("^zzz$", "", store)
	ff := core.CreateFlowFile([]byte("not matching"), map[string]string{"orig": "a"})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["orig"], "a")
		asserts.EqualInt(t, len(out.Attributes), 1)
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:556
func TestExtractTextInvalidRegexReturnsFailure(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewExtractText("[broken", "x", store)
	ff := core.CreateFlowFile([]byte("anything"), map[string]string{})
	switch _v := proc.Process(ff).(type) {
	case core.Failure:
		reason := _v.Reason
		asserts.Contains(t, reason, "invalid regex")
	default:
		_ = _v
		t.Errorf("expected Failure")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:566
func TestSplitTextSingleDelimiterYieldsMultipleFlowFilesWithSplitIndex(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewSplitText(",", 0, store)
	ff := core.CreateFlowFile([]byte("a,b,c"), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Multiple:
		ffs := _v.Ffs
		asserts.EqualInt(t, len(ffs), 3)
		idxs := []string{}
		for _, f := range ffs {
			idxs = append(idxs, f.Attributes["split.index"])
		}
		asserts.EqualString(t, idxs[0], "0")
		asserts.EqualString(t, idxs[1], "1")
		asserts.EqualString(t, idxs[2], "2")
		asserts.EqualString(t, ffs[0].Attributes["split.count"], "3")
	default:
		_ = _v
		t.Errorf("expected Multiple, got %v", result)
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:589
func TestSplitTextSingleChunkContentNoDelimiterMatchPassesThroughAsSingle(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewSplitText(",", 0, store)
	ff := core.CreateFlowFile([]byte("nosplit"), map[string]string{})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		_ = _v
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:599
func TestSplitTextHeaderLinesPrependedToEachSplit(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewSplitText(`
`, 1, store)
	ff := core.CreateFlowFile([]byte(`col1,col2
row1a,row1b
row2a,row2b`), map[string]string{})
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Multiple:
		ffs := _v.Ffs
		asserts.EqualInt(t, len(ffs), 2)
		for _, f := range ffs {
			switch _v := f.Content.(type) {
			case core.Raw:
				b := _v.Bytes
				asserts.IsTrue(t, strings.Contains(string(b), "col1,col2"), "header present")
			default:
				_ = _v
				t.Errorf("expected Raw")
			}
		}
	default:
		_ = _v
		t.Errorf("expected Multiple")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:620
func TestSplitTextInvalidDelimiterRegexReturnsFailure(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewSplitText("[broken", 0, store)
	ff := core.CreateFlowFile([]byte("a,b"), map[string]string{})
	switch _v := proc.Process(ff).(type) {
	case core.Failure:
		reason := _v.Reason
		asserts.Contains(t, reason, "invalid delimiter")
	default:
		_ = _v
		t.Errorf("expected Failure")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:632
func TestFilterAttributeDefaultModeRemoveDropsListedAttributes(t *testing.T) {
	proc := processors.NewFilterAttribute("", "secret;internal")
	ff := core.CreateFlowFile([]byte("x"), map[string]string{"public": "p", "secret": "s", "internal": "i"})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["public"], "p")
		asserts.IsFalse(t, func() bool { _, _ok := out.Attributes["secret"]; return _ok }(), "secret dropped")
		asserts.IsFalse(t, func() bool { _, _ok := out.Attributes["internal"]; return _ok }(), "internal dropped")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:647
func TestFilterAttributeModeKeepRetainsOnlyListedAttributes(t *testing.T) {
	proc := processors.NewFilterAttribute("keep", "env;region")
	ff := core.CreateFlowFile([]byte("x"), map[string]string{"env": "prod", "region": "us", "noise": "drop"})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualInt(t, len(out.Attributes), 2)
		asserts.EqualString(t, out.Attributes["env"], "prod")
		asserts.EqualString(t, out.Attributes["region"], "us")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:662
func TestFilterAttributeEmptyAttributeListIsANoOpRemovesNothingInRemoveMode(t *testing.T) {
	proc := processors.NewFilterAttribute("", "")
	ff := core.CreateFlowFile([]byte("x"), map[string]string{"k": "v"})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["k"], "v")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:671
func TestLogAttributePassesThroughUnchangedLogsAreObservational(t *testing.T) {
	proc := processors.NewLogAttribute("tagged")
	ff := core.CreateFlowFile([]byte("content"), map[string]string{"env": "dev"})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Id, ff.Id)
		asserts.EqualString(t, out.Attributes["env"], "dev")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:683
func TestPutFileWritesContentUnderOutputDirRecordsOutputPathOutputSize(t *testing.T) {
	store := core.NewMemoryContentStore()
	dir := "/tmp/zinc-flow-test/put-file-basic"
	proc := processors.NewPutFile(dir, "filename", "", ".dat", "raw", store)
	ff := core.CreateFlowFile([]byte("hello"), map[string]string{"filename": "greeting.txt"})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["output.path"], dir + "/greeting.txt")
		asserts.EqualString(t, out.Attributes["output.size"], "5")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:698
func TestPutFileMissingNamingAttributeFallsBackToCounterSuffix(t *testing.T) {
	store := core.NewMemoryContentStore()
	dir := "/tmp/zinc-flow-test/put-file-fallback"
	proc := processors.NewPutFile(dir, "filename", "batch-", ".dat", "raw", store)
	ff := core.CreateFlowFile([]byte("x"), map[string]string{})
	switch _v := proc.Process(ff).(type) {
	case core.Single:
		out := _v.Ff
		asserts.EqualString(t, out.Attributes["output.path"], dir + "/batch-1.dat")
	default:
		_ = _v
		t.Errorf("expected Single")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:712
func TestPutFileRecordsContentReturnsFailureCanTResolveToBytes(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewPutFile("/tmp/zinc-flow-test/put-file-rec", "filename", "", ".dat", "raw", store)
	schema := core.SchemaFromFields("test", []core.Field{})
	ff := core.NewFlowFile("ff-r", map[string]string{}, core.NewRecords(schema, []*core.GenericRecord{}), 0)
	switch _v := proc.Process(ff).(type) {
	case core.Failure:
		reason := _v.Reason
		asserts.Contains(t, reason, "Records")
	default:
		_ = _v
		t.Errorf("expected Failure on Records content")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/processors_test.zn:723
func TestPutStdoutRecordsContentReturnsFailureRawBytesResolutionUnsupported(t *testing.T) {
	store := core.NewMemoryContentStore()
	proc := processors.NewPutStdout("raw", store)
	schema := core.SchemaFromFields("test", []core.Field{})
	records := []*core.GenericRecord{}
	ff := core.NewFlowFile("ff-records", map[string]string{}, core.NewRecords(schema, records), 0)
	result := proc.Process(ff)
	switch _v := result.(type) {
	case core.Failure:
		reason := _v.Reason
		failFf := _v.Ff
		asserts.EqualString(t, failFf.Id, "ff-records")
		asserts.Contains(t, reason, "Records")
	default:
		_ = _v
		t.Errorf("expected Failure for Records content, got %v", result)
	}
}

