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

package core

// TopicOverloadPolicy is a policy to deal with an overloaded topic; so topic where there is no place to store new messages.
//
// This policy can only be used in combination with the ReliableTopic.
//
// The reliable topic uses a Ringbuffer to store the messages. A ringbuffer doesn't track where
// readers are, so therefore it has no concept of a slow consumers. This provides many advantages like high performance reads, but
// it also gives the ability to the reader to reread the same message multiple times in case of error.
//
// Because a ringbuffer has a capacity and to prevent that a fast producer overwrites the messages needed by a slow consumer, a
// time to live time can be set on the ringbuffer in server configuration.
// This policy controls how the publisher is going to deal with the situation that a ringbuffer is full
//  and the oldest item in the ringbuffer is not old enough to get overwritten.
//
// So keep in mind that this retention period keeps the messages in memory, even though it might be that all readers already
// have completed reading.
type TopicOverloadPolicy int

const (
	// TopicOverLoadPolicyDiscardOldest is a policy so that a message that has not expired can be overwritten.
	// No matter the retention period set, the overwrite will just overwrite the item.
	//
	// This can be a problem for slow consumers because they were promised a certain time window to process messages. But it will
	// benefit producers and fast consumers since they are able to continue. This policy sacrifices the slow producer in favor
	// of fast producers/consumers.
	TopicOverLoadPolicyDiscardOldest TopicOverloadPolicy = iota

	// TopicOverLoadPolicyDiscardNewest is a policy which discards a message that was to be published.
	TopicOverLoadPolicyDiscardNewest

	// TopicOverLoadPolicyBlock is a policy to block until there is a space in the ringbuffer.
	TopicOverLoadPolicyBlock

	// TopicOverLoadPolicyError is a policy so that the publish call fails immediately when there is no space.
	TopicOverLoadPolicyError
)
