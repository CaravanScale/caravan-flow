package core

import (
	"fmt"
	"sync"
	"time"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/dlq.zn:8
type DLQEntry struct {
	Id string
	FlowFile FlowFile
	SourceProcessor string
	SourceQueue string
	AttemptCount int
	LastError string
	ArrivedAt int64
}

func NewDLQEntry(id string, flowFile FlowFile, sourceProcessor string, sourceQueue string, attemptCount int, lastError string, arrivedAt int64) DLQEntry {
	return DLQEntry{Id: id, FlowFile: flowFile, SourceProcessor: sourceProcessor, SourceQueue: sourceQueue, AttemptCount: attemptCount, LastError: lastError, ArrivedAt: arrivedAt}
}

func (s DLQEntry) String() string {
	return fmt.Sprintf("DLQEntry(id=%v, flowFile=%v, sourceProcessor=%v, sourceQueue=%v, attemptCount=%v, lastError=%v, arrivedAt=%v)", s.Id, s.FlowFile, s.SourceProcessor, s.SourceQueue, s.AttemptCount, s.LastError, s.ArrivedAt)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/dlq.zn:11
type DLQ struct {
	entries map[string]DLQEntry
	mu sync.Mutex
	idCounter int64
}

func NewDLQ() *DLQ {
	return &DLQ{idCounter: int64(0), entries: map[string]DLQEntry{}}
}

func (s *DLQ) Add(ff FlowFile, sourceProc string, sourceQueue string, attempts int, error string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.idCounter = s.idCounter + 1
		entryId := "dlq-" + fmt.Sprint(s.idCounter)
		now := int64(time.Now().UnixNano())
		entry := NewDLQEntry(entryId, ff, sourceProc, sourceQueue, attempts, error, now)
		s.entries[entryId] = entry
	}()
}

func (s *DLQ) Get(id string) DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if func() bool { _, _ok := s.entries[id]; return _ok }() {
		return s.entries[id]
	}
	empty := make([]byte, 0)
	return NewDLQEntry("", NewFlowFile("", map[string]string{}, NewRaw(empty), 0), "", "", 0, "not found", int64(0))
}

func (s *DLQ) List() []DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []DLQEntry{}
	for _, entry := range s.entries {
		result = append(result, entry)
	}
	return result
	return nil
}

func (s *DLQ) Remove(id string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.entries, id)
	}()
}

func (s *DLQ) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
	return 0
}

func (s *DLQ) Replay(id string) FlowFile {
	s.mu.Lock()
	defer s.mu.Unlock()
	if func() bool { _, _ok := s.entries[id]; return _ok }() {
		entry := s.entries[id]
		delete(s.entries, id)
		return entry.FlowFile
	}
	empty := make([]byte, 0)
	return NewFlowFile("", map[string]string{}, NewRaw(empty), 0)
}

func (s *DLQ) ReplayAll() []DLQEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []DLQEntry{}
	for _, entry := range s.entries {
		result = append(result, entry)
	}
	s.entries = map[string]DLQEntry{}
	return result
	return nil
}


