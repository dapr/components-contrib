// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufferutil

import (
	"encoding/binary"
	"math"
)

func WriteInt32(buf []byte, pos int32, v int32, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint32(s, uint32(v))
	} else {
		binary.LittleEndian.PutUint32(s, uint32(v))
	}
}

func ReadInt32(buf []byte, pos int32, isBigEndian bool) int32 {
	if isBigEndian {
		return int32(binary.BigEndian.Uint32(buf[pos:]))
	}
	return int32(binary.LittleEndian.Uint32(buf[pos:]))

}

func WriteFloat32(buf []byte, pos int32, v float32, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint32(s, math.Float32bits(v))
	} else {
		binary.LittleEndian.PutUint32(s, math.Float32bits(v))
	}
}

func ReadFloat32(buf []byte, pos int32, isBigEndian bool) float32 {
	if isBigEndian {
		return math.Float32frombits(binary.BigEndian.Uint32(buf[pos:]))
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(buf[pos:]))
}

func WriteFloat64(buf []byte, pos int32, v float64, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint64(s, math.Float64bits(v))
	} else {
		binary.LittleEndian.PutUint64(s, math.Float64bits(v))
	}
}

func ReadFloat64(buf []byte, pos int32, isBigEndian bool) float64 {
	if isBigEndian {
		return math.Float64frombits(binary.BigEndian.Uint64(buf[pos:]))
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(buf[pos:]))
}

func WriteBool(buf []byte, pos int32, v bool) {
	var b byte = 1
	if v {
		buf[pos] = b
	} else {
		b = 0
		buf[pos] = b
	}
}

func ReadBool(buf []byte, pos int32) bool {
	return buf[pos] == 1
}

func WriteUInt8(buf []byte, pos int32, v uint8) {
	buf[pos] = v
}

func ReadUInt8(buf []byte, pos int32) uint8 {
	return buf[pos]
}

func WriteUInt16(buf []byte, pos int32, v uint16, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint16(s, v)
	} else {
		binary.LittleEndian.PutUint16(s, v)
	}
}

func ReadUInt16(buf []byte, pos int32, isBigEndian bool) uint16 {
	if isBigEndian {
		return binary.BigEndian.Uint16(buf[pos:])
	}
	return binary.LittleEndian.Uint16(buf[pos:])
}

func WriteInt16(buf []byte, pos int32, v int16, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint16(s, uint16(v))
	} else {
		binary.LittleEndian.PutUint16(s, uint16(v))
	}
}

func ReadInt16(buf []byte, pos int32, isBigEndian bool) int16 {
	if isBigEndian {
		return int16(binary.BigEndian.Uint16(buf[pos:]))
	}
	return int16(binary.LittleEndian.Uint16(buf[pos:]))
}

func WriteInt64(buf []byte, pos int32, v int64, isBigEndian bool) {
	s := buf[pos:]
	if isBigEndian {
		binary.BigEndian.PutUint64(s, uint64(v))
	} else {
		binary.LittleEndian.PutUint64(s, uint64(v))
	}
}

func ReadInt64(buf []byte, pos int32, isBigEndian bool) int64 {
	if isBigEndian {
		return int64(binary.BigEndian.Uint64(buf[pos:]))
	}
	return int64(binary.LittleEndian.Uint64(buf[pos:]))
}
