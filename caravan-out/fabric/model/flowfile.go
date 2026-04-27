package model

import (
	"bytes"
	"zinc-flow/core"
	"fmt"
)

const V3_MAGIC_LEN = 7

const MAX_VALUE_2_BYTES = 0xFFFF

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:11
type V3Result struct {
	Ff core.FlowFile
	NextOffset int
	ErrorMsg string
}

func NewV3Result(ff core.FlowFile, nextOffset int, errorMsg string) V3Result {
	return V3Result{Ff: ff, NextOffset: nextOffset, ErrorMsg: errorMsg}
}

func (s V3Result) String() string {
	return fmt.Sprintf("V3Result(ff=%v, nextOffset=%v, errorMsg=%v)", s.Ff, s.NextOffset, s.ErrorMsg)
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:21
func PackFlowFile(ff core.FlowFile, contentBytes []byte) []byte {
	buf := []byte("NiFiFF3")
	count := len(ff.Attributes)
	buf = append(buf, writeFieldLength(count)...)
	for key, value := range ff.Attributes {
		keyBytes := []byte(key)
		valBytes := []byte(value)
		buf = append(buf, writeFieldLength(len(keyBytes))...)
		buf = append(buf, keyBytes...)
		buf = append(buf, writeFieldLength(len(valBytes))...)
		buf = append(buf, valBytes...)
	}
	buf = append(buf, core.PackInt64BE(int64(len(contentBytes)))...)
	buf = append(buf, contentBytes...)
	return buf
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:46
func PackMultiple(flowfiles []core.FlowFile, contents [][]byte) []byte {
	buf := make([]byte, 0)
	for i := 0; i < len(flowfiles); i++ {
		buf = append(buf, PackFlowFile(flowfiles[i], contents[i])...)
	}
	return buf
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:57
func UnpackFlowFile(data []byte, offset int) V3Result {
	magic := []byte("NiFiFF3")
	if !bytes.Equal(data[offset:offset + V3_MAGIC_LEN], magic) {
		return NewV3Result(core.NewFlowFile("", map[string]string{}, core.NewRaw(make([]byte, 0)), 0), offset, fmt.Sprintf("invalid FlowFile V3 magic at offset %v", offset))
	}
	pos := offset + V3_MAGIC_LEN
	count := readFieldValue(data, pos)
	pos = readFieldNextOffset(data, pos)
	attributes := map[string]string{}
	for i := 0; i < count; i++ {
		keyLen := readFieldValue(data, pos)
		pos = readFieldNextOffset(data, pos)
		key := string(data[pos:pos + keyLen])
		pos = pos + keyLen
		valLen := readFieldValue(data, pos)
		pos = readFieldNextOffset(data, pos)
		val := string(data[pos:pos + valLen])
		pos = pos + valLen
		attributes[key] = val
	}
	contentLen := int(core.UnpackInt64BE(data, pos))
	pos = pos + 8
	content := data[pos:pos + contentLen]
	pos = pos + contentLen
	ff := core.CreateFlowFile(content, attributes)
	return NewV3Result(ff, pos, "")
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:95
func UnpackAll(data []byte) []core.FlowFile {
	flowfiles := []core.FlowFile{}
	pos := 0
	for pos < len(data) {
		result := UnpackFlowFile(data, pos)
		if result.ErrorMsg != "" {
			break
		}
		flowfiles = append(flowfiles, result.Ff)
		pos = result.NextOffset
	}
	return flowfiles
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:111
func writeFieldLength(value int) []byte {
	if value < MAX_VALUE_2_BYTES {
		return core.PackUint16BE(value)
	}
	sentinel := make([]byte, 2)
	sentinel[0] = 0xFF
	sentinel[1] = 0xFF
	return append(sentinel, core.PackUint32BE(value)...)
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:121
func readFieldValue(data []byte, offset int) int {
	val := core.UnpackUint16BE(data, offset)
	if val < MAX_VALUE_2_BYTES {
		return val
	}
	return core.UnpackUint32BE(data, offset + 2)
}

//line /home/vrjoshi/proj/zinc-flow/src/fabric/model/flowfile.zn:129
func readFieldNextOffset(data []byte, offset int) int {
	val := core.UnpackUint16BE(data, offset)
	if val < MAX_VALUE_2_BYTES {
		return offset + 2
	}
	return offset + 6
}

