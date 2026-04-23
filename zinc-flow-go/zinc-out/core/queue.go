package core

import (
	"fmt"
	"sync"
	"time"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/queue.zn:11
type QueueEntry struct {
	Id string
	FlowFile FlowFile
	ClaimedAt int64
	AttemptCount int
	SourceProcessor string
}

func NewQueueEntry(id string, flowFile FlowFile, claimedAt int64, attemptCount int, sourceProcessor string) QueueEntry {
	return QueueEntry{Id: id, FlowFile: flowFile, ClaimedAt: claimedAt, AttemptCount: attemptCount, SourceProcessor: sourceProcessor}
}

func (s QueueEntry) String() string {
	return fmt.Sprintf("QueueEntry(id=%v, flowFile=%v, claimedAt=%v, attemptCount=%v, sourceProcessor=%v)", s.Id, s.FlowFile, s.ClaimedAt, s.AttemptCount, s.SourceProcessor)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/queue.zn:14
type FlowQueue struct {
	name string
	maxCount int
	maxBytes int64
	visibilityTimeoutNanos int64
	items []QueueEntry
	head int
	invisible map[string]QueueEntry
	mu sync.Mutex
	currentBytes int64
	idCounter int64
}

func NewFlowQueue(name string, maxCount int, maxBytes int64, visibilityTimeoutNanos int64) *FlowQueue {
	return &FlowQueue{
		name: name,
		maxCount: maxCount,
		maxBytes: maxBytes,
		visibilityTimeoutNanos: visibilityTimeoutNanos,
		idCounter: int64(0),
		currentBytes: int64(0),
		items: []QueueEntry{},
		head: 0,
		invisible: map[string]QueueEntry{},
	}
}

func (s *FlowQueue) GetName() string {
	return s.name
}

func (s *FlowQueue) Offer(ff FlowFile) bool {
	ffBytes := int64(ContentSize(ff.Content))
	s.mu.Lock()
	defer s.mu.Unlock()
	visibleCount := len(s.items) - s.head
	total := visibleCount + len(s.invisible)
	if total < s.maxCount && (s.maxBytes == int64(0) || s.currentBytes + ffBytes <= s.maxBytes) {
		s.idCounter = s.idCounter + 1
		entryId := s.name + "-" + fmt.Sprint(s.idCounter)
		entry := NewQueueEntry(entryId, ff, int64(0), 0, "")
		s.items = append(s.items, entry)
		s.currentBytes = s.currentBytes + ffBytes
		return true
	}
	return false
}

func (s *FlowQueue) OfferWithSource(ff FlowFile, sourceProc string) bool {
	ffBytes := int64(ContentSize(ff.Content))
	s.mu.Lock()
	defer s.mu.Unlock()
	visibleCount := len(s.items) - s.head
	total := visibleCount + len(s.invisible)
	if total < s.maxCount && (s.maxBytes == int64(0) || s.currentBytes + ffBytes <= s.maxBytes) {
		s.idCounter = s.idCounter + 1
		entryId := s.name + "-" + fmt.Sprint(s.idCounter)
		entry := NewQueueEntry(entryId, ff, int64(0), 0, sourceProc)
		s.items = append(s.items, entry)
		s.currentBytes = s.currentBytes + ffBytes
		return true
	}
	return false
}

func (s *FlowQueue) Claim() QueueEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.head < len(s.items) {
		entry := s.items[s.head]
		s.head = s.head + 1
		s.compact()
		now := int64(time.Now().UnixNano())
		claimed := NewQueueEntry(entry.Id, entry.FlowFile, now, entry.AttemptCount, entry.SourceProcessor)
		s.invisible[claimed.Id] = claimed
		return claimed
	}
	empty := make([]byte, 0)
	return NewQueueEntry("", NewFlowFile("", map[string]string{}, NewRaw(empty), 0), int64(0), 0, "")
}

func (s *FlowQueue) Ack(entryId string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if func() bool { _, _ok := s.invisible[entryId]; return _ok }() {
			entry := s.invisible[entryId]
			s.currentBytes = s.currentBytes - int64(ContentSize(entry.FlowFile.Content))
			if s.currentBytes < int64(0) {
				s.currentBytes = int64(0)
			}
			delete(s.invisible, entryId)
		}
	}()
}

func (s *FlowQueue) Nack(entryId string) {
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if func() bool { _, _ok := s.invisible[entryId]; return _ok }() {
			entry := s.invisible[entryId]
			delete(s.invisible, entryId)
			updated := NewQueueEntry(entry.Id, entry.FlowFile, int64(0), entry.AttemptCount + 1, entry.SourceProcessor)
			s.items = append(s.items, updated)
		}
	}()
}

func (s *FlowQueue) HasCapacity() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	visibleCount := len(s.items) - s.head
	total := visibleCount + len(s.invisible)
	countOk := total < s.maxCount
	bytesOk := s.maxBytes == int64(0) || s.currentBytes < s.maxBytes
	return countOk && bytesOk
	return false
}

func (s *FlowQueue) GetBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentBytes
	return 0
}

func (s *FlowQueue) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items) - s.head + len(s.invisible)
	return 0
}

func (s *FlowQueue) VisibleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items) - s.head
	return 0
}

func (s *FlowQueue) InvisibleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.invisible)
	return 0
}

func (s *FlowQueue) compact() {
	live := len(s.items) - s.head
	if s.head > 0 && s.head >= live {
		compacted := []QueueEntry{}
		i := s.head
		for i < len(s.items) {
			compacted = append(compacted, s.items[i])
			i = i + 1
		}
		s.items = compacted
		s.head = 0
	}
}

func (s *FlowQueue) StartReaper() {
	go func() {
		for true {
			time.Sleep(1 * time.Second)
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				if len(s.invisible) > 0 {
					now := int64(time.Now().UnixNano())
					expired := []string{}
					for id, entry := range s.invisible {
						if now - entry.ClaimedAt > s.visibilityTimeoutNanos {
							expired = append(expired, id)
						}
					}
					for _, id := range expired {
						entry := s.invisible[id]
						delete(s.invisible, id)
						updated := NewQueueEntry(entry.Id, entry.FlowFile, int64(0), entry.AttemptCount + 1, entry.SourceProcessor)
						s.items = append(s.items, updated)
					}
				}
			}()
		}
	}()
}


