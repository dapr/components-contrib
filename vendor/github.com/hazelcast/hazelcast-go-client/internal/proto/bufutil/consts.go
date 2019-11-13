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

package bufutil

const (
	ServiceNameAtomicLong                  = "hz:impl:atomicLongService"
	ServiceNameAtomicReference             = "hz:impl:atomicReferenceService"
	ServiceNameCountDownLatch              = "hz:impl:countDownLatchService"
	ServiceNameIDGenerator                 = "hz:impl:idGeneratorService"
	ServiceNameExecutor                    = "hz:impl:executorService"
	ServiceNameLock                        = "hz:impl:lockService"
	ServiceNameList                        = "hz:impl:listService"
	ServiceNameMultiMap                    = "hz:impl:multiMapService"
	ServiceNameMap                         = "hz:impl:mapService"
	ServiceNameReliableTopic               = "hz:impl:reliableTopicService"
	ServiceNameReplicatedMap               = "hz:impl:replicatedMapService"
	ServiceNameRingbufferService           = "hz:impl:ringbufferService"
	ServiceNameSemaphore                   = "hz:impl:semaphoreService"
	ServiceNameSet                         = "hz:impl:setService"
	ServiceNameQueue                       = "hz:impl:queueService"
	ServiceNameTopic                       = "hz:impl:topicService"
	ServiceNameIDGeneratorAtomicLongPrefix = "hz:atomic:idGenerator:"
	ServiceNamePNCounter                   = "hz:impl:PNCounterService"
)

type MessageType uint16

//MESSAGE TYPES
const (
	MessageTypeException MessageType = 109
)

type ErrorCode int32

//ERROR CODES
const (
	ErrorCodeUndefined                        ErrorCode = 0
	ErrorCodeArrayIndexOutOfBounds            ErrorCode = 1
	ErrorCodeArrayStore                       ErrorCode = 2
	ErrorCodeAuthentication                   ErrorCode = 3
	ErrorCodeCache                            ErrorCode = 4
	ErrorCodeCacheLoader                      ErrorCode = 5
	ErrorCodeCacheNotExists                   ErrorCode = 6
	ErrorCodeCacheWriter                      ErrorCode = 7
	ErrorCodeCallerNotMember                  ErrorCode = 8
	ErrorCodeCancellation                     ErrorCode = 9
	ErrorCodeClassCast                        ErrorCode = 10
	ErrorCodeClassNotFound                    ErrorCode = 11
	ErrorCodeConcurrentModification           ErrorCode = 12
	ErrorCodeConfigMismatch                   ErrorCode = 13
	ErrorCodeConfiguration                    ErrorCode = 14
	ErrorCodeDistributedObjectDestroyed       ErrorCode = 15
	ErrorCodeDuplicateInstanceName            ErrorCode = 16
	ErrorCodeEOF                              ErrorCode = 17
	ErrorCodeEntryProcessor                   ErrorCode = 18
	ErrorCodeExecution                        ErrorCode = 19
	ErrorCodeHazelcast                        ErrorCode = 20
	ErrorCodeHazelcastInstanceNotActive       ErrorCode = 21
	ErrorCodeHazelcastOverLoad                ErrorCode = 22
	ErrorCodeHazelcastSerialization           ErrorCode = 23
	ErrorCodeIO                               ErrorCode = 24
	ErrorCodeIllegalArgument                  ErrorCode = 25
	ErrorCodeIllegalAccessException           ErrorCode = 26
	ErrorCodeIllegalAccessError               ErrorCode = 27
	ErrorCodeIllegalMonitorState              ErrorCode = 28
	ErrorCodeIllegalState                     ErrorCode = 29
	ErrorCodeIllegalThreadState               ErrorCode = 30
	ErrorCodeIndexOutOfBounds                 ErrorCode = 31
	ErrorCodeInterrupted                      ErrorCode = 32
	ErrorCodeInvalidAddress                   ErrorCode = 33
	ErrorCodeInvalidConfiguration             ErrorCode = 34
	ErrorCodeMemberLeft                       ErrorCode = 35
	ErrorCodeNegativeArraySize                ErrorCode = 36
	ErrorCodeNoSuchElement                    ErrorCode = 37
	ErrorCodeNotSerializable                  ErrorCode = 38
	ErrorCodeNilPointer                       ErrorCode = 39
	ErrorCodeOperationTimeout                 ErrorCode = 40
	ErrorCodePartitionMigrating               ErrorCode = 41
	ErrorCodeQuery                            ErrorCode = 42
	ErrorCodeQueryResultSizeExceeded          ErrorCode = 43
	ErrorCodeQuorum                           ErrorCode = 44
	ErrorCodeReachedMaxSize                   ErrorCode = 45
	ErrorCodeRejectedExecution                ErrorCode = 46
	ErrorCodeRemoteMapReduce                  ErrorCode = 47
	ErrorCodeResponseAlreadySent              ErrorCode = 48
	ErrorCodeRetryableHazelcast               ErrorCode = 49
	ErrorCodeRetryableIO                      ErrorCode = 50
	ErrorCodeRuntime                          ErrorCode = 51
	ErrorCodeSecurity                         ErrorCode = 52
	ErrorCodeSocket                           ErrorCode = 53
	ErrorCodeStaleSequence                    ErrorCode = 54
	ErrorCodeTargetDisconnected               ErrorCode = 55
	ErrorCodeTargetNotMember                  ErrorCode = 56
	ErrorCodeTimeout                          ErrorCode = 57
	ErrorCodeTopicOverload                    ErrorCode = 58
	ErrorCodeTopologyChanged                  ErrorCode = 59
	ErrorCodeTransaction                      ErrorCode = 60
	ErrorCodeTransactionNotActive             ErrorCode = 61
	ErrorCodeTransactionTimedOut              ErrorCode = 62
	ErrorCodeURISyntax                        ErrorCode = 63
	ErrorCodeUTFDataFormat                    ErrorCode = 64
	ErrorCodeUnsupportedOperation             ErrorCode = 65
	ErrorCodeWrongTarget                      ErrorCode = 66
	ErrorCodeXA                               ErrorCode = 67
	ErrorCodeAccessControl                    ErrorCode = 68
	ErrorCodeLogin                            ErrorCode = 69
	ErrorCodeUnsupportedCallback              ErrorCode = 70
	ErrorCodeNoDataMember                     ErrorCode = 71
	ErrorCodeReplicatedMapCantBeCreated       ErrorCode = 72
	ErrorCodeMaxMessageSizeExceeded           ErrorCode = 73
	ErrorCodeWANReplicationQueueFull          ErrorCode = 74
	ErrorCodeAssertionError                   ErrorCode = 75
	ErrorCodeOutOfMemoryError                 ErrorCode = 76
	ErrorCodeStackOverflowError               ErrorCode = 77
	ErrorCodeNativeOutOfMemoryError           ErrorCode = 78
	ErrorCodeNotFound                         ErrorCode = 79
	ErrorCodeStaleTaskID                      ErrorCode = 80
	ErrorCodeDuplicateTask                    ErrorCode = 81
	ErrorCodeStaleTask                        ErrorCode = 82
	ErrorCodeLocalMemberReset                 ErrorCode = 83
	ErrorCodeIndeterminateOperationState      ErrorCode = 84
	ErrorCodeFlakeIDNodeIDOutOfRangeException ErrorCode = 85
	ErrorCodeTargetNotReplicaException        ErrorCode = 86
	ErrorCodeMutationDisallowedException      ErrorCode = 87
	ErrorCodeConsistencyLostException         ErrorCode = 88
)

/*
Event Response Constants
*/
const (
	EventMember                 = 200
	EventMemberList             = 201
	EventMemberAttributeChange  = 202
	EventEntry                  = 203
	EventItem                   = 204
	EventTopic                  = 205
	EventPartitionLost          = 206
	EventDistributedObject      = 207
	EventCacheInvalidation      = 208
	EventMapPartitionLost       = 209
	EventCache                  = 210
	EventCacheBatchInvalidation = 211
	// ENTERPRISE
	EventQueryCacheSingle = 212
	EventQueryCacheBatch  = 213

	EventCachePartitionLost    = 214
	EventIMapInvalidation      = 215
	EventIMapBatchInvalidation = 216
)

const (
	ByteSizeInBytes    = 1
	BoolSizeInBytes    = 1
	Uint8SizeInBytes   = 1
	Int16SizeInBytes   = 2
	Uint16SizeInBytes  = 2
	Int32SizeInBytes   = 4
	Float32SizeInBytes = 4
	Float64SizeInBytes = 8
	Int64SizeInBytes   = 8

	Version            = 0
	BeginFlag    uint8 = 0x80
	EndFlag      uint8 = 0x40
	BeginEndFlag uint8 = BeginFlag | EndFlag
	ListenerFlag uint8 = 0x01

	PayloadOffset = 18
	SizeOffset    = 0

	FrameLengthFieldOffset   = 0
	VersionFieldOffset       = FrameLengthFieldOffset + Int32SizeInBytes
	FlagsFieldOffset         = VersionFieldOffset + ByteSizeInBytes
	TypeFieldOffset          = FlagsFieldOffset + ByteSizeInBytes
	CorrelationIDFieldOffset = TypeFieldOffset + Int16SizeInBytes
	PartitionIDFieldOffset   = CorrelationIDFieldOffset + Int64SizeInBytes
	DataOffsetFieldOffset    = PartitionIDFieldOffset + Int32SizeInBytes
	HeaderSize               = DataOffsetFieldOffset + Int16SizeInBytes

	NilArrayLength = -1
)

const (
	EntryEventAdded        int32 = 1
	EntryEventRemoved      int32 = 2
	EntryEventUpdated      int32 = 4
	EntryEventEvicted      int32 = 8
	MapEventEvicted        int32 = 16
	MapEventCleared        int32 = 32
	EntryEventMerged       int32 = 64
	EntryEventExpired      int32 = 128
	EntryEventInvalidation int32 = 256
)

const (
	ItemAdded   int32 = 1
	ItemRemoved int32 = 2
)

const (
	NilKeyIsNotAllowed        string = "nil key is not allowed"
	NilKeysAreNotAllowed      string = "nil keys collection is not allowed"
	NilValueIsNotAllowed      string = "nil value is not allowed"
	NilPredicateIsNotAllowed  string = "predicate should not be nil"
	NilMapIsNotAllowed        string = "nil map is not allowed"
	NilArgIsNotAllowed        string = "nil arg is not allowed"
	NilSliceIsNotAllowed      string = "nil slice is not allowed"
	NilListenerIsNotAllowed   string = "nil listener is not allowed"
	NilAggregatorIsNotAllowed string = "aggregator should not be nil"
	NilProjectionIsNotAllowed string = "projection should not be nil"
)
