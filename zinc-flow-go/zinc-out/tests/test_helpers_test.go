package tests

import (
	"zinc-flow/core"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/test_helpers_test.zn:9
func MkContext() *core.ProcessorContext {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	cp.Enable()
	ctx := core.NewProcessorContext()
	ctx.AddProvider(cp)
	return ctx
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/test_helpers_test.zn:20
func MkScopedContext() *core.ScopedContext {
	store := core.NewMemoryContentStore()
	cp := core.NewContentProvider("content", store)
	cp.Enable()
	providers := map[string]core.Provider{}
	providers["content"] = cp
	return core.NewScopedContext(providers)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/test_helpers_test.zn:31
func JsonArrayFixture() []byte {
	return []byte("[{\"name\": \"Alice\", \"amount\": 42}]")
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/tests/test_helpers_test.zn:35
func JsonGeoFixture() []byte {
	return []byte("[{\"city\": \"Portland\"}]")
}

