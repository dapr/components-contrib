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

package proto

import (
	"encoding/binary"
	"unicode/utf8"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
)

// ClientMessage is the carrier framed data as defined below.
// Any request parameter, response or event data will be carried in the payload.
//	0                   1                   2                   3
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|R|                      Frame Length                           |
//	+-------------+---------------+---------------------------------+
//	|  Version    |B|E|  Flags    |               Type              |
//	+-------------+---------------+---------------------------------+
//	|                                                               |
//	+                       CorrelationID                           +
//	|                                                               |
//	+---------------------------------------------------------------+
//	|                        PartitionID                            |
//	+-----------------------------+---------------------------------+
//	|        Data Offset          |                                 |
//	+-----------------------------+                                 |
//	|                      Message Payload Data                    ...
//	|                                                              ...
type ClientMessage struct {
	Buffer      []byte
	writeIndex  int32
	readIndex   int32
	IsRetryable bool
}

/*

	Constructor

*/

func NewClientMessage(buffer []byte, payloadSize int) *ClientMessage {
	clientMessage := new(ClientMessage)
	if buffer != nil {
		//Message has a buffer so it will be decoded.
		clientMessage.Buffer = buffer
		clientMessage.readIndex = bufutil.HeaderSize
	} else {
		//Client message that will be encoded.
		clientMessage.Buffer = make([]byte, bufutil.HeaderSize+payloadSize)
		clientMessage.SetDataOffset(bufutil.HeaderSize)
		clientMessage.writeIndex = bufutil.HeaderSize
	}
	clientMessage.IsRetryable = false
	return clientMessage
}

func (m *ClientMessage) CloneMessage() *ClientMessage {
	newBuffer := make([]byte, len(m.Buffer))
	copy(newBuffer, m.Buffer)
	copiedMessage := NewClientMessage(newBuffer, 0)
	copiedMessage.IsRetryable = m.IsRetryable
	return copiedMessage
}

func (m *ClientMessage) FrameLength() int32 {
	return int32(binary.LittleEndian.Uint32(m.Buffer[bufutil.FrameLengthFieldOffset:bufutil.VersionFieldOffset]))
}

func (m *ClientMessage) SetFrameLength(v int32) {
	binary.LittleEndian.PutUint32(m.Buffer[bufutil.FrameLengthFieldOffset:bufutil.VersionFieldOffset], uint32(v))
}

func (m *ClientMessage) SetVersion(v uint8) {
	m.Buffer[bufutil.VersionFieldOffset] = byte(v)
}

func (m *ClientMessage) Flags() uint8 {
	return m.Buffer[bufutil.FlagsFieldOffset]
}

func (m *ClientMessage) SetFlags(v uint8) {
	m.Buffer[bufutil.FlagsFieldOffset] = byte(v)
}

func (m *ClientMessage) AddFlags(v uint8) {
	m.Buffer[bufutil.FlagsFieldOffset] = m.Buffer[bufutil.FlagsFieldOffset] | byte(v)
}

func (m *ClientMessage) HasFlags(flags uint8) uint8 {
	value := m.Flags() & flags
	if value == flags {
		return value
	}
	return 0
}

func (m *ClientMessage) MessageType() bufutil.MessageType {
	return bufutil.MessageType(binary.LittleEndian.Uint16(m.Buffer[bufutil.TypeFieldOffset:bufutil.CorrelationIDFieldOffset]))
}

func (m *ClientMessage) SetMessageType(v bufutil.MessageType) {
	binary.LittleEndian.PutUint16(m.Buffer[bufutil.TypeFieldOffset:bufutil.CorrelationIDFieldOffset], uint16(v))
}

func (m *ClientMessage) CorrelationID() int64 {
	return int64(binary.LittleEndian.Uint64(m.Buffer[bufutil.CorrelationIDFieldOffset:bufutil.PartitionIDFieldOffset]))
}

func (m *ClientMessage) SetCorrelationID(val int64) {
	binary.LittleEndian.PutUint64(m.Buffer[bufutil.CorrelationIDFieldOffset:bufutil.PartitionIDFieldOffset], uint64(val))
}

func (m *ClientMessage) PartitionID() int32 {
	return int32(binary.LittleEndian.Uint32(m.Buffer[bufutil.PartitionIDFieldOffset:bufutil.DataOffsetFieldOffset]))
}

func (m *ClientMessage) SetPartitionID(val int32) {
	binary.LittleEndian.PutUint32(m.Buffer[bufutil.PartitionIDFieldOffset:bufutil.DataOffsetFieldOffset], uint32(val))
}

func (m *ClientMessage) DataOffset() uint16 {
	return binary.LittleEndian.Uint16(m.Buffer[bufutil.DataOffsetFieldOffset:bufutil.HeaderSize])
}

func (m *ClientMessage) SetDataOffset(v uint16) {
	binary.LittleEndian.PutUint16(m.Buffer[bufutil.DataOffsetFieldOffset:bufutil.HeaderSize], v)
}

func (m *ClientMessage) writeOffset() int32 {
	return int32(m.DataOffset()) + m.writeIndex
}

func (m *ClientMessage) readOffset() int32 {
	return m.readIndex
}

/*
	PAYLOAD
*/

func (m *ClientMessage) AppendByte(v uint8) {
	m.Buffer[m.writeIndex] = byte(v)
	m.writeIndex += bufutil.ByteSizeInBytes
}

func (m *ClientMessage) AppendUint8(v uint8) {
	m.Buffer[m.writeIndex] = byte(v)
	m.writeIndex += bufutil.ByteSizeInBytes
}

func (m *ClientMessage) AppendInt32(v int32) {
	binary.LittleEndian.PutUint32(m.Buffer[m.writeIndex:m.writeIndex+bufutil.Int32SizeInBytes], uint32(v))
	m.writeIndex += bufutil.Int32SizeInBytes
}

func (m *ClientMessage) AppendData(v serialization.Data) {
	m.AppendByteArray(v.Buffer())
}

func (m *ClientMessage) AppendByteArray(arr []byte) {
	length := int32(len(arr))
	//length
	m.AppendInt32(length)
	//copy content
	copy(m.Buffer[m.writeIndex:m.writeIndex+length], arr)
	m.writeIndex += length
}

func (m *ClientMessage) AppendInt64(v int64) {
	binary.LittleEndian.PutUint64(m.Buffer[m.writeIndex:m.writeIndex+bufutil.Int64SizeInBytes], uint64(v))
	m.writeIndex += bufutil.Int64SizeInBytes
}

func (m *ClientMessage) AppendString(str string) {
	if utf8.ValidString(str) {
		m.AppendByteArray([]byte(str))
	} else {
		buff := make([]byte, 0, len(str)*3)
		n := 0
		for _, b := range str {
			n += utf8.EncodeRune(buff[n:], rune(b))
		}
		//append fixed size slice
		m.AppendByteArray(buff[0:n])
	}
}

func (m *ClientMessage) AppendBool(v bool) {
	if v {
		m.AppendByte(1)
	} else {
		m.AppendByte(0)
	}
}

/*
	PAYLOAD READ
*/

func (m *ClientMessage) ReadInt32() int32 {
	int := int32(binary.LittleEndian.Uint32(m.Buffer[m.readOffset() : m.readOffset()+bufutil.Int32SizeInBytes]))
	m.readIndex += bufutil.Int32SizeInBytes
	return int
}

func (m *ClientMessage) ReadInt64() int64 {
	int64 := int64(binary.LittleEndian.Uint64(m.Buffer[m.readOffset() : m.readOffset()+bufutil.Int64SizeInBytes]))
	m.readIndex += bufutil.Int64SizeInBytes
	return int64
}

func (m *ClientMessage) ReadUint8() uint8 {
	byte := byte(m.Buffer[m.readOffset()])
	m.readIndex += bufutil.ByteSizeInBytes
	return byte
}

func (m *ClientMessage) ReadBool() bool {
	if m.ReadUint8() == 1 {
		return true
	} else {
		return false
	}
}

func (m *ClientMessage) ReadString() string {
	str := string(m.ReadByteArray())
	return str
}

func (m *ClientMessage) ReadData() serialization.Data {
	return spi.NewData(m.ReadByteArray())
}

func (m *ClientMessage) ReadByteArray() []byte {
	length := m.ReadInt32()
	result := m.Buffer[m.readOffset() : m.readOffset()+length]
	m.readIndex += length
	return result
}

/*
	Helpers
*/
func (m *ClientMessage) UpdateFrameLength() {
	m.SetFrameLength(int32(m.writeIndex))
}

func (m *ClientMessage) Accumulate(newMsg *ClientMessage) {
	start := newMsg.DataOffset()
	end := newMsg.FrameLength()
	m.Buffer = append(m.Buffer, newMsg.Buffer[start:end]...)
	m.SetFrameLength(int32(len(m.Buffer)))
}

func (m *ClientMessage) IsComplete() bool {
	return (m.readOffset() >= bufutil.HeaderSize) && (m.readOffset() == m.FrameLength())
}
