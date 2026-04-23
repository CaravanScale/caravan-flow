package core

import (
	"zinc-flow/fabric/router"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/session.zn:7
type ProcessSession struct {
	source *FlowQueue
	processor ProcessorFn
	processorName string
	irs *router.RulesEngine
	destQueues map[string]*FlowQueue
	dlq *DLQ
	maxRetries int
}

func NewProcessSession(source *FlowQueue, processor ProcessorFn, processorName string, irs *router.RulesEngine, destQueues map[string]*FlowQueue, dlq *DLQ, maxRetries int) *ProcessSession {
	return &ProcessSession{
		source: source,
		processor: processor,
		processorName: processorName,
		irs: irs,
		destQueues: destQueues,
		dlq: dlq,
		maxRetries: maxRetries,
	}
}

func (s *ProcessSession) Execute() bool {
	entry := s.source.Claim()
	if entry.Id == "" {
		return false
	}
	if entry.AttemptCount >= s.maxRetries {
		s.dlq.Add(entry.FlowFile, s.processorName, s.source.GetName(), entry.AttemptCount, "max retries exceeded")
		s.source.Ack(entry.Id)
		return true
	}
	result := s.processor.Process(entry.FlowFile)
	switch _v := result.(type) {
	case Single:
		ff := _v.Ff
		if s.routeResult(ff, entry) {
			s.source.Ack(entry.Id)
		}
	case Multiple:
		ffs := _v.Ffs
		allRouted := true
		for _, ff := range ffs {
			if !s.routeResult(ff, entry) {
				allRouted = false
				break
			}
		}
		if allRouted {
			s.source.Ack(entry.Id)
		}
	case Routed:
		ff := _v.Ff
		if s.routeResult(ff, entry) {
			s.source.Ack(entry.Id)
		}
	case Dropped:
		_ = _v
		s.source.Ack(entry.Id)
	case Failure:
		reason := _v.Reason
		ff := _v.Ff
		s.dlq.Add(ff, s.processorName, s.source.GetName(), entry.AttemptCount, reason)
		s.source.Ack(entry.Id)
	default:
		_ = _v
		panic("unreachable")
	}
	return true
}

func (s *ProcessSession) routeResult(ff FlowFile, entry QueueEntry) bool {
	destinations := s.irs.GetDestinations(ff.Attributes)
	if len(destinations) == 0 {
		return true
	}
	for _, dest := range destinations {
		queueName := dest.Endpoint
		if func() bool { _, _ok := s.destQueues[queueName]; return _ok }() {
			if !s.destQueues[queueName].HasCapacity() {
				s.source.Nack(entry.Id)
				return false
			}
		}
	}
	for _, dest := range destinations {
		queueName := dest.Endpoint
		if func() bool { _, _ok := s.destQueues[queueName]; return _ok }() {
			s.destQueues[queueName].OfferWithSource(ff, s.processorName)
		}
	}
	return true
}


