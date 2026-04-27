package tests

import (
	"testing"
	"zinc-flow/fabric/registry"
	"zinc-flow/processors"
	"zinc-flow/fabric/runtime"
	"zinc-flow/fabric/source"
	"github.com/ZincScale/zinc-stdlib/asserts"
)

//line /home/vrjoshi/proj/zinc-flow/tests/mvp_e2e_test.zn:25
func TestMVPPipelineE2eGenerateUpdateRouteSink(t *testing.T) {
	ctx := MkContext()
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tag-stage", "UpdateAttribute", map[string]string{"key": "stage", "value": "processed"})
	fab.AddProcessor("router", "RouteOnAttribute", map[string]string{"routes": "dev: env EQ dev; prod: env EQ prod"})
	devSink := NewRecordSink("dev")
	prodSink := NewRecordSink("prod")
	unmatchedSink := NewRecordSink("unmatched")
	fab.AddProcessorInstance("stdout-dev", devSink, []string{})
	fab.AddProcessorInstance("stdout-prod", prodSink, []string{})
	fab.AddProcessorInstance("stdout-unmatched", unmatchedSink, []string{})
	fab.Connect("tag-stage", "success", []string{"router"})
	fab.Connect("router", "dev", []string{"stdout-dev"})
	fab.Connect("router", "prod", []string{"stdout-prod"})
	fab.Connect("router", "unmatched", []string{"stdout-unmatched"})
	fab.ComputeEntryPoints()
	src := source.NewGenerateFlowFile("gen", 0, "ping", "text/plain", "env:dev", 3)
	batch := src.PollOnce()
	asserts.EqualInt(t, len(batch), 3)
	for _, ff := range batch {
		fab.Execute(ff, "tag-stage")
	}
	asserts.EqualInt(t, devSink.Count(), 3)
	asserts.EqualInt(t, prodSink.Count(), 0)
	asserts.EqualInt(t, unmatchedSink.Count(), 0)
	for _, ff := range devSink.Captured {
		asserts.EqualString(t, ff.Attributes["stage"], "processed")
		asserts.EqualString(t, ff.Attributes["env"], "dev")
		asserts.EqualString(t, ff.Attributes["source"], "gen")
	}
}

//line /home/vrjoshi/proj/zinc-flow/tests/mvp_e2e_test.zn:85
func TestMVPPipelineE2eProdTrafficLandsOnProdBranchDevStaysEmpty(t *testing.T) {
	ctx := MkContext()
	reg := registry.NewRegistry()
	processors.RegisterBuiltins(reg)
	fab := runtime.NewFabric(reg, ctx)
	fab.AddProcessor("tag-stage", "UpdateAttribute", map[string]string{"key": "stage", "value": "processed"})
	fab.AddProcessor("router", "RouteOnAttribute", map[string]string{"routes": "dev: env EQ dev; prod: env EQ prod"})
	devSink := NewRecordSink("dev")
	prodSink := NewRecordSink("prod")
	fab.AddProcessorInstance("stdout-dev", devSink, []string{})
	fab.AddProcessorInstance("stdout-prod", prodSink, []string{})
	fab.Connect("tag-stage", "success", []string{"router"})
	fab.Connect("router", "dev", []string{"stdout-dev"})
	fab.Connect("router", "prod", []string{"stdout-prod"})
	fab.ComputeEntryPoints()
	src := source.NewGenerateFlowFile("gen-prod", 0, "ping", "", "env:prod", 2)
	batch := src.PollOnce()
	for _, ff := range batch {
		fab.Execute(ff, "tag-stage")
	}
	asserts.EqualInt(t, devSink.Count(), 0)
	asserts.EqualInt(t, prodSink.Count(), 2)
}

