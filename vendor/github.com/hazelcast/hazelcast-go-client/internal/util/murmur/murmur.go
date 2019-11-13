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

package murmur

import (
	"encoding/binary"
)

var defaultSeed uint32 = 0x01000193

func Default3A(key []byte, offset int32, len int) int32 {
	return M3A(key, offset, len, defaultSeed)
}

// M3A computes MurmurHash3 for x86, 32-bit (MurmurHash3_x86_32)
func M3A(key []byte, offset int32, len int, seed uint32) int32 {
	var h1 = seed
	var c1 uint32 = 0xcc9e2d51
	var c2 uint32 = 0x1b873593
	var roundedEnd = offset + int32(uint32(len)&uint32(0xfffffffc))
	// body
	for i := offset; i < roundedEnd; i += 4 {
		k1 := binary.LittleEndian.Uint32(key[i:]) // TODO Validate

		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	// tail
	var tail = key[roundedEnd:] // TODO Validate
	var k1 uint32
	switch len & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	//finalization
	h1 ^= uint32(len)

	h1 = fmix32(h1)

	return int32(h1)
}

func rotl32(x uint32, r uint8) uint32 {
	return (x << r) | (x >> (32 - r))
}

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}

func HashToIndex(hash int32, length int32) int32 {
	if uint32(hash) == 0x80000000 {
		return 0
	}
	if hash < 0 {
		hash = -1 * hash
	}
	return hash % length
}
