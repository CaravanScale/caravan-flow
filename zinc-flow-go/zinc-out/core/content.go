package core

import (
	"fmt"
)

const CONTENT_CLAIM_THRESHOLD = 256 * 1024

type Content interface {
	isContent()
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/content.zn:10
type Raw struct {
	Bytes []byte
}

func NewRaw(bytes []byte) Raw {
	return Raw{Bytes: bytes}
}

func (s Raw) String() string {
	return fmt.Sprintf("Raw(bytes=%v)", s.Bytes)
}

func (Raw) isContent() {}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/content.zn:14
type Records struct {
	Schema Schema
	Rows []*GenericRecord
}

func NewRecords(schema Schema, rows []*GenericRecord) Records {
	return Records{Schema: schema, Rows: rows}
}

func (s Records) String() string {
	return fmt.Sprintf("Records(schema=%v, rows=%v)", s.Schema, s.Rows)
}

func (Records) isContent() {}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/content.zn:17
type Claim struct {
	ClaimId string
	Size int64
}

func NewClaim(claimId string, size int64) Claim {
	return Claim{ClaimId: claimId, Size: size}
}

func (s Claim) String() string {
	return fmt.Sprintf("Claim(claimId=%v, size=%v)", s.ClaimId, s.Size)
}

func (Claim) isContent() {}


//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/content.zn:21
func ContentSize(c Content) int64 {
	switch _v := c.(type) {
	case Raw:
		bytes := _v.Bytes
		return int64(len(bytes))
	case Records:
		rows := _v.Rows
		return int64(len(rows))
	case Claim:
		size := _v.Size
		return size
	default:
		_ = _v
		panic("unreachable")
	}
	return 0
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/content.zn:36
func IsRecord(c Content) bool {
	switch _v := c.(type) {
	case Records:
		_ = _v
		return true
	case Raw:
		_ = _v
		return false
	case Claim:
		_ = _v
		return false
	default:
		_ = _v
		panic("unreachable")
	}
	return false
}

