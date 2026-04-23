package tests

import (
	"strings"
	"zinc-flow/fabric/model"
	"testing"
	"zinc-flow/core"
	"github.com/ZincScale/zinc-stdlib/asserts"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:12
func TestFlowFileBasicsCreateWithAttributeWithContent(t *testing.T) {
	attrs := map[string]string{"type": "order", "source": "test"}
	ff := core.CreateFlowFile([]byte("hello"), attrs)
	asserts.IsTrue(t, strings.HasPrefix(ff.Id, "ff-"), "id starts with ff-")
	asserts.EqualString(t, ff.Attributes["type"], "order")
	asserts.EqualString(t, ff.Attributes["source"], "test")
	ff2 := core.WithAttribute(ff, "env", "dev")
	asserts.EqualString(t, ff2.Attributes["env"], "dev")
	asserts.IsTrue(t, ff2.Id == ff.Id, "same id after withAttribute")
	asserts.EqualString(t, ff2.Attributes["type"], "order")
	ff3 := core.WithContent(ff, core.NewRaw([]byte("new data")))
	asserts.IsTrue(t, ff3.Id == ff.Id, "same id after withContent")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:30
func TestContentTypesRawRecordsClaim(t *testing.T) {
	raw := core.NewRaw([]byte("data"))
	asserts.IsFalse(t, core.IsRecord(raw), "raw is not record")
	asserts.EqualInt(t, int(core.ContentSize(raw)), 4)
	schema := core.SchemaFromFields("test", []core.Field{})
	rec := core.NewRecords(schema, []*core.GenericRecord{})
	asserts.IsTrue(t, core.IsRecord(rec), "records is record")
	claim := core.NewClaim("claim-1", int64(1024))
	asserts.IsFalse(t, core.IsRecord(claim), "claim is not record")
	asserts.EqualInt(t, int(core.ContentSize(claim)), 1024)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:46
func TestContentStoreStoreRetrieveDeleteMaybeOffloadResolve(t *testing.T) {
	store := core.NewMemoryContentStore()
	claimId := store.Store([]byte("hello world"))
	asserts.IsTrue(t, claimId != "", "claim id not empty")
	asserts.IsTrue(t, store.Exists(claimId), "claim exists")
	retrieved := store.Retrieve(claimId)
	asserts.EqualString(t, string(retrieved), "hello world")
	store.Delete(claimId)
	asserts.IsFalse(t, store.Exists(claimId), "deleted claim gone")
	smallContent := core.MaybeOffload(store, []byte("small"))
	asserts.IsTrue(t, !core.IsRecord(smallContent), "small stays raw")
	resolved := core.Resolve(store, core.NewRaw([]byte("test")))
	asserts.EqualString(t, string(resolved.Data), "test")
	asserts.EqualString(t, resolved.ErrorMsg, "")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:66
func TestResolveErrorsMissingClaimRecordsUnresolvableRoundtrip(t *testing.T) {
	store := core.NewMemoryContentStore()
	resolved := core.Resolve(store, core.NewClaim("nonexistent", int64(100)))
	asserts.EqualInt(t, len(resolved.Data), 0)
	schema := core.SchemaFromFields("test", []core.Field{})
	resolvedRec := core.Resolve(store, core.NewRecords(schema, []*core.GenericRecord{}))
	asserts.IsTrue(t, resolvedRec.ErrorMsg != "", "records resolve has error")
	claimId := store.Store([]byte("claimed data"))
	resolvedClaim := core.Resolve(store, core.NewClaim(claimId, int64(12)))
	asserts.EqualString(t, string(resolvedClaim.Data), "claimed data")
	asserts.EqualString(t, resolvedClaim.ErrorMsg, "")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:84
func TestV3RoundtripSingleFlowFileAttributesAndContentPreserved(t *testing.T) {
	attrs := map[string]string{"filename": "test.txt", "type": "doc"}
	ff := core.CreateFlowFile([]byte("Hello, NiFi!"), attrs)
	store := core.NewMemoryContentStore()
	resolved := core.Resolve(store, ff.Content)
	packed := model.PackFlowFile(ff, resolved.Data)
	asserts.IsTrue(t, len(packed) > 0, "packed not empty")
	asserts.EqualString(t, string(packed[0:7]), "NiFiFF3")
	result := model.UnpackFlowFile(packed, 0)
	asserts.EqualString(t, result.ErrorMsg, "")
	asserts.EqualString(t, result.Ff.Attributes["filename"], "test.txt")
	asserts.EqualString(t, result.Ff.Attributes["type"], "doc")
	resolvedBack := core.Resolve(store, result.Ff.Content)
	asserts.EqualString(t, string(resolvedBack.Data), "Hello, NiFi!")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:103
func TestV3MultipleRoundtripTwoFlowFilesPreserveOrderAndAttrs(t *testing.T) {
	ff1 := core.CreateFlowFile([]byte("first"), map[string]string{"index": "1"})
	ff2 := core.CreateFlowFile([]byte("second"), map[string]string{"index": "2"})
	store := core.NewMemoryContentStore()
	r1 := core.Resolve(store, ff1.Content)
	r2 := core.Resolve(store, ff2.Content)
	packed := model.PackMultiple([]core.FlowFile{ff1, ff2}, [][]byte{r1.Data, r2.Data})
	all := model.UnpackAll(packed)
	asserts.EqualInt(t, len(all), 2)
	asserts.EqualString(t, all[0].Attributes["index"], "1")
	asserts.EqualString(t, all[1].Attributes["index"], "2")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:122
func TestV3EmptyAttributesZeroAttrFlowFileRoundtrips(t *testing.T) {
	ff := core.CreateFlowFile([]byte("payload"), map[string]string{})
	store := core.NewMemoryContentStore()
	resolved := core.Resolve(store, ff.Content)
	packed := model.PackFlowFile(ff, resolved.Data)
	result := model.UnpackFlowFile(packed, 0)
	asserts.EqualString(t, result.ErrorMsg, "")
	asserts.EqualInt(t, len(result.Ff.Attributes), 0)
	resolvedBack := core.Resolve(store, result.Ff.Content)
	asserts.EqualString(t, string(resolvedBack.Data), "payload")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/core_test.zn:136
func TestV3EmptyContentEmptyBytesFlowFileSurvivesRoundtrip(t *testing.T) {
	ff := core.CreateFlowFile(make([]byte, 0), map[string]string{"tag": "empty"})
	store := core.NewMemoryContentStore()
	resolved := core.Resolve(store, ff.Content)
	packed := model.PackFlowFile(ff, resolved.Data)
	result := model.UnpackFlowFile(packed, 0)
	asserts.EqualString(t, result.ErrorMsg, "")
	asserts.EqualString(t, result.Ff.Attributes["tag"], "empty")
	resolvedBack := core.Resolve(store, result.Ff.Content)
	asserts.EqualInt(t, len(resolvedBack.Data), 0)
}

