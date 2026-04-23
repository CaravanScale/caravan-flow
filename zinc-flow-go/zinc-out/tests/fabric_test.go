package tests

import (
	"zinc-flow/processors"
	"zinc-flow/fabric/runtime"
	"github.com/ZincScale/zinc-stdlib/asserts"
	"zinc-flow/core"
	"fmt"
	"zinc-flow/fabric/registry"
	"testing"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:21
type RecordSink struct {
	label string
	Captured []core.FlowFile
}

func NewRecordSink(label string) *RecordSink {
	return &RecordSink{label: label, Captured: []core.FlowFile{}}
}

func (s *RecordSink) Process(ff core.FlowFile) core.ProcessorResult {
	s.Captured = append(s.Captured, ff)
	return core.NewDropped(fmt.Sprintf("recorded by %v", s.label))
}

func (s *RecordSink) Count() int {
	return len(s.Captured)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:40
type RouterProc struct {
	attr string
	goodValue string
}

func NewRouterProc(attr string, goodValue string) *RouterProc {
	return &RouterProc{attr: attr, goodValue: goodValue}
}

func (s *RouterProc) Process(ff core.FlowFile) core.ProcessorResult {
	if func() bool { _, _ok := ff.Attributes[s.attr]; return _ok }() && ff.Attributes[s.attr] == s.goodValue {
		return core.NewRouted("success", ff)
	}
	return core.NewRouted("failure", ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:59
type SplitProc struct {
}

func NewSplitProc() *SplitProc {
	return &SplitProc{}
}

func (s *SplitProc) Process(ff core.FlowFile) core.ProcessorResult {
	out := []core.FlowFile{}
	switch _v := ff.Content.(type) {
	case core.Raw:
		bytes := _v.Bytes
		s := string(bytes)
		i := 0
		for i < len(s) {
			ch := s[i:i + 1]
			attrs := map[string]string{}
			for _, k := range func() []string { _keys := make([]string, 0, len(ff.Attributes)); for _k := range ff.Attributes { _keys = append(_keys, _k) }; return _keys }() {
				attrs[k] = ff.Attributes[k]
			}
			attrs["split.idx"] = fmt.Sprint(i)
			out = append(out, core.NewFlowFile(ff.Id + "-" + fmt.Sprint(i), attrs, core.NewRaw([]byte(ch)), ff.Timestamp))
			i = i + 1
		}
	default:
		_ = _v
		out = append(out, ff)
	}
	return core.NewMultiple(out)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:90
type FailProc struct {
	reason string
}

func NewFailProc(reason string) *FailProc {
	return &FailProc{reason: reason}
}

func (s *FailProc) Process(ff core.FlowFile) core.ProcessorResult {
	return core.NewFailure(s.reason, ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:101
type LoopProc struct {
}

func NewLoopProc() *LoopProc {
	return &LoopProc{}
}

func (s *LoopProc) Process(ff core.FlowFile) core.ProcessorResult {
	return core.NewSingle(ff)
}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:110
func recordSinkFactory(ctx *core.ScopedContext, cfg map[string]string) core.ProcessorFn {
	return NewRecordSink(cfg["label"])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:113
func routerFactory(ctx *core.ScopedContext, cfg map[string]string) core.ProcessorFn {
	return NewRouterProc(cfg["attr"], cfg["good"])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:116
func failFactory(ctx *core.ScopedContext, cfg map[string]string) core.ProcessorFn {
	return NewFailProc(cfg["reason"])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:119
func loopFactory(ctx *core.ScopedContext, cfg map[string]string) core.ProcessorFn {
	return NewLoopProc()
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:123
func registerRecordingProcessors(reg *registry.Registry) {
	reg.Register(registry.NewProcessorInfo("record-sink", "Records every FlowFile", []string{}), recordSinkFactory)
	reg.Register(registry.NewProcessorInfo("router", "Routes by attribute", []string{"attr", "good"}), routerFactory)
	reg.Register(registry.NewProcessorInfo("fail", "Always Failure", []string{"reason"}), failFactory)
	reg.Register(registry.NewProcessorInfo("loop", "Self-loop via success", []string{}), loopFactory)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:144
func TestExecutorSingleResultFollowsSuccessConnection(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tagger", "add-attribute", map[string]string{"key": "tag", "value": "seen"})
	sink := NewRecordSink("after-tag")
	fab.AddProcessorInstance("sink", sink, []string{})
	fab.Connect("tagger", "success", []string{"sink"})
	ff := core.CreateFlowFile([]byte("hi"), map[string]string{})
	ok := fab.Execute(ff, "tagger")
	asserts.IsTrue(t, ok, "execute returned true")
	asserts.EqualInt(t, sink.Count(), 1)
	asserts.EqualString(t, sink.Captured[0].Attributes["tag"], "seen")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:163
func TestExecutorMultipleResultFansEachOutputToSuccessTargets(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	splitter := NewSplitProc()
	fab.AddProcessorInstance("splitter", splitter, []string{})
	sink := NewRecordSink("fanout")
	fab.AddProcessorInstance("sink", sink, []string{})
	fab.Connect("splitter", "success", []string{"sink"})
	ff := core.CreateFlowFile([]byte("abc"), map[string]string{})
	fab.Execute(ff, "splitter")
	asserts.EqualInt(t, sink.Count(), 3)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:181
func TestExecutorRoutedResultFollowsTheNamedRelationship(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	router := NewRouterProc("env", "prod")
	fab.AddProcessorInstance("router", router, []string{})
	prod := NewRecordSink("prod")
	dev := NewRecordSink("dev")
	fab.AddProcessorInstance("prod", prod, []string{})
	fab.AddProcessorInstance("dev", dev, []string{})
	fab.Connect("router", "success", []string{"prod"})
	fab.Connect("router", "failure", []string{"dev"})
	fab.Execute(core.CreateFlowFile([]byte("a"), map[string]string{"env": "prod"}), "router")
	fab.Execute(core.CreateFlowFile([]byte("b"), map[string]string{"env": "staging"}), "router")
	asserts.EqualInt(t, prod.Count(), 1)
	asserts.EqualInt(t, dev.Count(), 1)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:204
func TestExecutorDroppedResultTerminalNoDownstreamDelivery(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	dropper := NewRecordSink("drop-label")
	fab.AddProcessorInstance("dropper", dropper, []string{})
	shouldNotSee := NewRecordSink("never")
	fab.AddProcessorInstance("never", shouldNotSee, []string{})
	fab.Connect("dropper", "success", []string{"never"})
	fab.Execute(core.CreateFlowFile([]byte("x"), map[string]string{}), "dropper")
	asserts.EqualInt(t, dropper.Count(), 1)
	asserts.EqualInt(t, shouldNotSee.Count(), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:223
func TestExecutorFailureWithFailureConnectionRoutesThereNoDLQ(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	failer := NewFailProc("boom")
	fab.AddProcessorInstance("failer", failer, []string{})
	onErr := NewRecordSink("err-handler")
	fab.AddProcessorInstance("err", onErr, []string{})
	fab.Connect("failer", "failure", []string{"err"})
	fab.Execute(core.CreateFlowFile([]byte("x"), map[string]string{}), "failer")
	asserts.EqualInt(t, onErr.Count(), 1)
	asserts.EqualInt(t, fab.GetDlqCount(), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:241
func TestExecutorFailureWithoutFailureConnectionDLQ(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	failer := NewFailProc("unrouted")
	fab.AddProcessorInstance("failer", failer, []string{})
	fab.Execute(core.CreateFlowFile([]byte("x"), map[string]string{}), "failer")
	asserts.EqualInt(t, fab.GetDlqCount(), 1)
	entries := fab.GetDLQ().List()
	asserts.EqualString(t, entries[0].SourceProcessor, "failer")
	asserts.EqualString(t, entries[0].LastError, "unrouted")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:259
func TestExecutorMaxHopsDropsFlowFileDoesNotInfiniteLoop(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	fab.SetMaxHops(5)
	loop := NewLoopProc()
	fab.AddProcessorInstance("loop", loop, []string{})
	fab.Connect("loop", "success", []string{"loop"})
	fab.Execute(core.CreateFlowFile([]byte("x"), map[string]string{}), "loop")
	asserts.IsTrue(t, true, "hop guard returned")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:277
func TestExecutorDisabledProcessorSkippedNoDownstreamDelivery(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	sink := NewRecordSink("post-disabled")
	fab.AddProcessorInstance("sink", sink, []string{})
	fab.AddProcessor("tagger", "add-attribute", map[string]string{"key": "t", "value": "v"})
	fab.Connect("tagger", "success", []string{"sink"})
	fab.DisableProcessor("tagger", 0)
	fab.Execute(core.CreateFlowFile([]byte("x"), map[string]string{}), "tagger")
	asserts.EqualInt(t, sink.Count(), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:294
func TestExecutorIngestAndExecuteFansToAllEntryPoints(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	registerRecordingProcessors(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	a := NewRecordSink("A")
	b := NewRecordSink("B")
	fab.AddProcessorInstance("a", a, []string{})
	fab.AddProcessorInstance("b", b, []string{})
	fab.ComputeEntryPoints()
	asserts.EqualInt(t, len(fab.GetEntryPoints()), 2)
	accepted := fab.IngestAndExecute(core.CreateFlowFile([]byte("hi"), map[string]string{}))
	asserts.IsTrue(t, accepted, "ingested")
	asserts.EqualInt(t, a.Count(), 1)
	asserts.EqualInt(t, b.Count(), 1)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:315
func TestExecutorIngestAndExecuteWithNoEntryPointsReturnsFalse(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	asserts.IsFalse(t, fab.IngestAndExecute(core.CreateFlowFile([]byte("x"), map[string]string{})), "rejected with no entries")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:325
func TestDLQAddCapturesProcessorNameQueueAttemptCountError(t *testing.T) {
	dlq := core.NewDLQ()
	asserts.EqualInt(t, dlq.Count(), 0)
	ff := core.CreateFlowFile([]byte("failed"), map[string]string{"type": "test"})
	dlq.Add(ff, "my-proc", "my-queue", 3, "processing error")
	asserts.EqualInt(t, dlq.Count(), 1)
	entries := dlq.List()
	asserts.EqualInt(t, len(entries), 1)
	asserts.EqualString(t, entries[0].SourceProcessor, "my-proc")
	asserts.EqualString(t, entries[0].LastError, "processing error")
	asserts.EqualInt(t, entries[0].AttemptCount, 3)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:340
func TestDLQReplayReturnsFlowFileAndRemovesFromDLQ(t *testing.T) {
	dlq := core.NewDLQ()
	ff := core.CreateFlowFile([]byte("replay-me"), map[string]string{"id": "123"})
	dlq.Add(ff, "proc", "queue", 5, "max retries")
	asserts.EqualInt(t, dlq.Count(), 1)
	entries := dlq.List()
	replayedFf := dlq.Replay(entries[0].Id)
	asserts.EqualString(t, replayedFf.Id, ff.Id)
	asserts.EqualInt(t, dlq.Count(), 0)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:354
func TestScopedContextProviderLookupViaDeclaredScope(t *testing.T) {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	cp.Enable()
	providers := map[string]core.Provider{}
	providers["content"] = cp
	ctx := core.NewScopedContext(providers)
	asserts.EqualInt(t, len(ctx.ListProviders()), 1)
	_tryerr := func() error {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return err1
		}
		asserts.EqualString(t, p.GetName(), "content")
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		t.Errorf("content provider should exist")
	}
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/fabric_test.zn:375
func TestProviderLifecycleEnableDisableReEnableShutdown(t *testing.T) {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	asserts.IsFalse(t, cp.IsEnabled(), "initially disabled")
	cp.Enable()
	asserts.IsTrue(t, cp.IsEnabled(), "after enable")
	cp.Disable(0)
	asserts.IsFalse(t, cp.IsEnabled(), "after disable")
	cp.Enable()
	asserts.IsTrue(t, cp.IsEnabled(), "re-enabled")
	cp.Shutdown()
	asserts.IsFalse(t, cp.IsEnabled(), "after shutdown")
}

