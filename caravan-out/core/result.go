package core

import (
	"fmt"
)

type ProcessorResult interface {
	isProcessorResult()
}

//line /home/vrjoshi/proj/zinc-flow/src/core/result.zn:5
type Single struct {
	Ff FlowFile
}

func NewSingle(ff FlowFile) Single {
	return Single{Ff: ff}
}

func (s Single) String() string {
	return fmt.Sprintf("Single(ff=%v)", s.Ff)
}

func (Single) isProcessorResult() {}

//line /home/vrjoshi/proj/zinc-flow/src/core/result.zn:6
type Multiple struct {
	Ffs []FlowFile
}

func NewMultiple(ffs []FlowFile) Multiple {
	return Multiple{Ffs: ffs}
}

func (s Multiple) String() string {
	return fmt.Sprintf("Multiple(ffs=%v)", s.Ffs)
}

func (Multiple) isProcessorResult() {}

//line /home/vrjoshi/proj/zinc-flow/src/core/result.zn:7
type Routed struct {
	Route string
	Ff FlowFile
}

func NewRouted(route string, ff FlowFile) Routed {
	return Routed{Route: route, Ff: ff}
}

func (s Routed) String() string {
	return fmt.Sprintf("Routed(route=%v, ff=%v)", s.Route, s.Ff)
}

func (Routed) isProcessorResult() {}

//line /home/vrjoshi/proj/zinc-flow/src/core/result.zn:8
type Dropped struct {
	Reason string
}

func NewDropped(reason string) Dropped {
	return Dropped{Reason: reason}
}

func (s Dropped) String() string {
	return fmt.Sprintf("Dropped(reason=%v)", s.Reason)
}

func (Dropped) isProcessorResult() {}

//line /home/vrjoshi/proj/zinc-flow/src/core/result.zn:9
type Failure struct {
	Reason string
	Ff FlowFile
}

func NewFailure(reason string, ff FlowFile) Failure {
	return Failure{Reason: reason, Ff: ff}
}

func (s Failure) String() string {
	return fmt.Sprintf("Failure(reason=%v, ff=%v)", s.Reason, s.Ff)
}

func (Failure) isProcessorResult() {}


type ProcessorFn interface {
	Process(ff FlowFile) ProcessorResult
}

