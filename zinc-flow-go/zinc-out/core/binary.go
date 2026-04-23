package core

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:5
func PackUint16BE(value int) []byte {
	buf := make([]byte, 2)
	buf[0] = byte(value >> 8)
	buf[1] = byte(value & 0xFF)
	return buf
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:13
func UnpackUint16BE(data []byte, offset int) int {
	return int(data[offset]) << 8 | int(data[offset + 1])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:18
func PackUint32BE(value int) []byte {
	buf := make([]byte, 4)
	buf[0] = byte(value >> 24)
	buf[1] = byte(value >> 16 & 0xFF)
	buf[2] = byte(value >> 8 & 0xFF)
	buf[3] = byte(value & 0xFF)
	return buf
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:28
func UnpackUint32BE(data []byte, offset int) int {
	return int(data[offset]) << 24 | int(data[offset + 1]) << 16 | int(data[offset + 2]) << 8 | int(data[offset + 3])
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:36
func PackInt64BE(value int64) []byte {
	buf := make([]byte, 8)
	buf[0] = byte(value >> 56)
	buf[1] = byte(value >> 48 & 0xFF)
	buf[2] = byte(value >> 40 & 0xFF)
	buf[3] = byte(value >> 32 & 0xFF)
	buf[4] = byte(value >> 24 & 0xFF)
	buf[5] = byte(value >> 16 & 0xFF)
	buf[6] = byte(value >> 8 & 0xFF)
	buf[7] = byte(value & 0xFF)
	return buf
}

//line /home/vrjoshi/proj/caravan-flow/zinc-flow-go/src/core/binary.zn:50
func UnpackInt64BE(data []byte, offset int) int64 {
	return int64(data[offset]) << 56 | int64(data[offset + 1]) << 48 | int64(data[offset + 2]) << 40 | int64(data[offset + 3]) << 32 | int64(data[offset + 4]) << 24 | int64(data[offset + 5]) << 16 | int64(data[offset + 6]) << 8 | int64(data[offset + 7])
}

