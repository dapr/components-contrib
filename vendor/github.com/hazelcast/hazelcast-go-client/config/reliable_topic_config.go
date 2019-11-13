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

package config

import "github.com/hazelcast/hazelcast-go-client/core"

const (

	// defaultReadBatchSize is the default read batch size.
	defaultReadBatchSize = 10

	// defaultTopicOverloadPolicy is the default slow consumer policy.
	defaultTopicOverloadPolicy = core.TopicOverLoadPolicyBlock
)

// ReliableTopicConfig contains the ReliableTopic configuration for a client.
type ReliableTopicConfig struct {
	name                string
	readBatchSize       int32
	topicOverLoadPolicy core.TopicOverloadPolicy
}

// NewReliableTopicConfig returns a ReliableTopicConfig with the given name.
func NewReliableTopicConfig(name string) *ReliableTopicConfig {
	cfg := &ReliableTopicConfig{
		readBatchSize:       defaultReadBatchSize,
		topicOverLoadPolicy: defaultTopicOverloadPolicy,
		name:                name,
	}
	return cfg
}

// ReadBatchSize return the readBatchSize of this config.
func (r *ReliableTopicConfig) ReadBatchSize() int32 {
	return r.readBatchSize
}

// Name returns the name of this config.
func (r *ReliableTopicConfig) Name() string {
	return r.name
}

// TopicOverloadPolicy returns the TopicOverloadPolicy for this config.
func (r *ReliableTopicConfig) TopicOverloadPolicy() core.TopicOverloadPolicy {
	return r.topicOverLoadPolicy
}

// SetTopicOverloadPolicy sets the TopicOverloadPolicy as the given TopicOverloadPolicy.
func (r *ReliableTopicConfig) SetTopicOverloadPolicy(policy core.TopicOverloadPolicy) {
	// TODO :: check NIL
	r.topicOverLoadPolicy = policy
}

// SetReadBatchSize sets the read batch size for this config.
// The given read batch size should be positive.
func (r *ReliableTopicConfig) SetReadBatchSize(readBatchSize int32) {
	if readBatchSize <= 0 {
		panic("readBatchSize should be positive.")
	}
	r.readBatchSize = readBatchSize
}
