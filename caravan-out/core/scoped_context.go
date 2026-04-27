package core

import (
	"github.com/ZincScale/zinc-stdlib/exceptions"
	"fmt"
)

//line /home/vrjoshi/proj/zinc-flow/src/core/scoped_context.zn:8
type ScopedContext struct {
	providers map[string]Provider
}

func NewScopedContext(providers map[string]Provider) *ScopedContext {
	return &ScopedContext{providers: providers}
}

func (s *ScopedContext) GetProvider(name string) (Provider, error) {
	if !func() bool { _, _ok := s.providers[name]; return _ok }() {
		return nil, exceptions.NewConfigException(fmt.Sprintf("provider '%v' not in scope for this processor", name))
	}
	p := s.providers[name]
	if !p.IsEnabled() {
		return nil, exceptions.NewIllegalStateException(fmt.Sprintf("provider '%v' is not enabled", name))
	}
	return p, nil
}

func (s *ScopedContext) ListProviders() []string {
	return func() []string { _keys := make([]string, 0, len(s.providers)); for _k := range s.providers { _keys = append(_keys, _k) }; return _keys }()
}


