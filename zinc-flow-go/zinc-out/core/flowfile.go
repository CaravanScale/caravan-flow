package core

import (
	"fmt"
	"time"
)

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:6
type FlowFile struct {
	Id string
	Attributes map[string]string
	Content Content
	Timestamp int
}

func NewFlowFile(id string, attributes map[string]string, content Content, timestamp int) FlowFile {
	return FlowFile{Id: id, Attributes: attributes, Content: content, Timestamp: timestamp}
}

func (s FlowFile) String() string {
	return fmt.Sprintf("FlowFile(id=%v, attributes=%v, content=%v, timestamp=%v)", s.Id, s.Attributes, s.Content, s.Timestamp)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:14
func FlowFileId() string {
	return fmt.Sprintf("ff-%v", time.Now().UnixNano())
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:19
func CreateFlowFile(data []byte, attributes map[string]string) FlowFile {
	id := FlowFileId()
	return NewFlowFile(id, attributes, NewRaw(data), int(time.Now().Unix()))
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:25
func CreateRecordFlowFile(schema Schema, rows []*GenericRecord, attributes map[string]string) FlowFile {
	id := FlowFileId()
	return NewFlowFile(id, attributes, NewRecords(schema, rows), int(time.Now().Unix()))
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:31
func CreateFlowFileWithContent(content Content, attributes map[string]string) FlowFile {
	id := FlowFileId()
	return NewFlowFile(id, attributes, content, int(time.Now().Unix()))
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:37
func WithAttribute(ff FlowFile, key string, value string) FlowFile {
	attrs := ff.Attributes
	attrs[key] = value
	return NewFlowFile(ff.Id, attrs, ff.Content, ff.Timestamp)
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/flowfile.zn:44
func WithContent(ff FlowFile, content Content) FlowFile {
	return NewFlowFile(ff.Id, ff.Attributes, content, ff.Timestamp)
}

