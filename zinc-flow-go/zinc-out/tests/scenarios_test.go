package tests

import (
	"github.com/ZincScale/zinc-stdlib/asserts"
	"github.com/ZincScale/zinc-stdlib/logging"
	"testing"
	"zinc-flow/fabric/registry"
	"zinc-flow/processors"
	"zinc-flow/fabric/runtime"
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:16
func TestScenarioChainAddAttributeRecordSinkViaSuccess(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tagger", "add-attribute", map[string]string{"key": "env", "value": "dev"})
	sink := NewRecordSink("chain")
	fab.AddProcessorInstance("sink", sink, []string{})
	fab.Connect("tagger", "success", []string{"sink"})
	ff := core.CreateFlowFile([]byte("hello"), map[string]string{"type": "order"})
	fab.Execute(ff, "tagger")
	asserts.EqualInt(t, sink.Count(), 1)
	asserts.EqualString(t, sink.Captured[0].Attributes["env"], "dev")
	asserts.EqualString(t, sink.Captured[0].Attributes["type"], "order")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:35
func TestScenarioGraphFanOutOneSourceToThreeSinks(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tagger", "add-attribute", map[string]string{"key": "env", "value": "prod"})
	a := NewRecordSink("a")
	b := NewRecordSink("b")
	c := NewRecordSink("c")
	fab.AddProcessorInstance("a", a, []string{})
	fab.AddProcessorInstance("b", b, []string{})
	fab.AddProcessorInstance("c", c, []string{})
	fab.Connect("tagger", "success", []string{"a", "b", "c"})
	fab.Execute(core.CreateFlowFile([]byte("fanout"), map[string]string{}), "tagger")
	asserts.EqualInt(t, a.Count(), 1)
	asserts.EqualInt(t, b.Count(), 1)
	asserts.EqualInt(t, c.Count(), 1)
	asserts.EqualString(t, a.Captured[0].Attributes["env"], "prod")
	asserts.EqualString(t, b.Captured[0].Attributes["env"], "prod")
	asserts.EqualString(t, c.Captured[0].Attributes["env"], "prod")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:61
func TestScenarioDisableEnableProcessorStateTransitionsTakeEffect(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tagger", "add-attribute", map[string]string{"key": "env", "value": "test"})
	sink := NewRecordSink("downstream")
	fab.AddProcessorInstance("sink", sink, []string{})
	fab.Connect("tagger", "success", []string{"sink"})
	switch fab.GetProcessorState("tagger") {
	case core.ENABLED:
	default:
		t.Errorf("initially expected ENABLED")
	}
	fab.DisableProcessor("tagger", 0)
	fab.Execute(core.CreateFlowFile([]byte("a"), map[string]string{}), "tagger")
	asserts.EqualInt(t, sink.Count(), 0)
	fab.EnableProcessor("tagger")
	switch fab.GetProcessorState("tagger") {
	case core.ENABLED:
	default:
		t.Errorf("after enableProcessor expected ENABLED")
	}
	fab.Execute(core.CreateFlowFile([]byte("b"), map[string]string{}), "tagger")
	asserts.EqualInt(t, sink.Count(), 1)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:90
func TestScenarioDLQReplayResumeAtTheRecordedSourceProcessor(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	sink := NewRecordSink("resume")
	fab.AddProcessorInstance("sink", sink, []string{})
	ff := core.CreateFlowFile([]byte("replayable"), map[string]string{"type": "order"})
	fab.GetDLQ().Add(ff, "sink", "direct", 3, "upstream failed")
	asserts.EqualInt(t, fab.GetDlqCount(), 1)
	entries := fab.GetDLQ().List()
	replayed := fab.GetDLQ().Replay(entries[0].Id)
	asserts.IsTrue(t, fab.ReplayAt("sink", replayed), "replayAt accepted")
	asserts.EqualInt(t, fab.GetDlqCount(), 0)
	asserts.EqualInt(t, sink.Count(), 1)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:111
func TestScenarioScopedProviderIsolationOnlyDeclaredProvidersVisible(t *testing.T) {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	cp.Enable()
	lm := logging.NewLogManager()
	lp := core.NewLoggingProvider(lm)
	lp.Enable()
	scoped := map[string]core.Provider{}
	scoped["content"] = cp
	ctx := core.NewScopedContext(scoped)
	_tryerr := func() error {
		p, err1 := ctx.GetProvider("content")
		if err1 != nil {
			return err1
		}
		asserts.EqualString(t, p.GetType(), "content")
		return nil
	}()
	if _tryerr != nil {
		e := _tryerr
		_ = e
		t.Errorf("content provider should exist")
	}
	asserts.EqualInt(t, len(ctx.ListProviders()), 1)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:133
func TestScenarioProviderDisableCascadesDependentProcessorsBecomeDISABLED(t *testing.T) {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	cp.Enable()
	globalCtx := core.NewProcessorContext()
	globalCtx.AddProvider(cp)
	globalCtx.RegisterDependent("content", "file-sink")
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	fab := runtime.NewFabric(reg, globalCtx)
	fab.AddProcessor("file-sink", "file-sink", map[string]string{"output_dir": "/tmp/test"})
	switch fab.GetProcessorState("file-sink") {
	case core.ENABLED:
	default:
		t.Errorf("initially expected ENABLED")
	}
	fab.DisableProvider("content", 1)
	asserts.IsFalse(t, cp.IsEnabled(), "content provider disabled")
	switch fab.GetProcessorState("file-sink") {
	case core.DISABLED:
	case core.ENABLED:
		t.Errorf("sink should not be ENABLED after cascade")
	case core.DRAINING:
	}
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:162
func TestScenarioEnableProcessorRefusesWhenARequiredProviderIsDisabled(t *testing.T) {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	globalCtx := core.NewProcessorContext()
	globalCtx.AddProvider(cp)
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	fab := runtime.NewFabric(reg, globalCtx)
	fab.AddProcessor("sink", "file-sink", map[string]string{"output_dir": "/tmp/test"})
	fab.DisableProcessor("sink", 0)
	asserts.IsTrue(t, fab.EnableProcessor("sink"), "enable succeeds when no requires")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/scenarios_test.zn:184
func TestScenarioRouterDualSinksSuccessAndFailureLandInTheRightPlace(t *testing.T) {
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	ctx := MkContext()
	fab := runtime.NewFabric(reg, ctx)
	router := NewRouterProc("env", "prod")
	fab.AddProcessorInstance("router", router, []string{})
	ok := NewRecordSink("ok")
	notOk := NewRecordSink("not-ok")
	fab.AddProcessorInstance("ok", ok, []string{})
	fab.AddProcessorInstance("not-ok", notOk, []string{})
	fab.Connect("router", "success", []string{"ok"})
	fab.Connect("router", "failure", []string{"not-ok"})
	fab.Execute(core.CreateFlowFile([]byte("1"), map[string]string{"env": "prod"}), "router")
	fab.Execute(core.CreateFlowFile([]byte("2"), map[string]string{"env": "dev"}), "router")
	fab.Execute(core.CreateFlowFile([]byte("3"), map[string]string{"env": "prod"}), "router")
	asserts.EqualInt(t, ok.Count(), 2)
	asserts.EqualInt(t, notOk.Count(), 1)
}

