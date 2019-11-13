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

const (
	queueOffer               = 0x0301
	queuePut                 = 0x0302
	queueSize                = 0x0303
	queueRemove              = 0x0304
	queuePoll                = 0x0305
	queueTake                = 0x0306
	queuePeek                = 0x0307
	queueIterator            = 0x0308
	queueDrainTo             = 0x0309
	queueDrainToMaxSize      = 0x030a
	queueContains            = 0x030b
	queueContainsAll         = 0x030c
	queueCompareAndRemoveAll = 0x030d
	queueCompareAndRetainAll = 0x030e
	queueClear               = 0x030f
	queueAddAll              = 0x0310
	queueAddListener         = 0x0311
	queueRemoveListener      = 0x0312
	queueRemainingCapacity   = 0x0313
	queueIsEmpty             = 0x0314
)
