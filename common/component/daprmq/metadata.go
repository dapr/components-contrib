/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package daprmq

import (
	"errors"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	kitmd "github.com/dapr/kit/metadata"
)

// Receive modes.
const (
	// ReceiveModeStream consumes over a Consume stream: the server keeps maxActiveMessages delivered,
	// refills as they settle and renews their locks.
	ReceiveModeStream = "stream"
	// ReceiveModePoll consumes by polling DequeueLocked in batches and renewing locks itself.
	ReceiveModePoll = "poll"
)

// Metadata is the DaprMQ pub/sub components' metadata.
type Metadata struct {
	// DaprMQ REST API base URL.
	HTTPEndpoint string `mapstructure:"httpEndpoint"`
	// DaprMQ gRPC API address.
	GRPCEndpoint string `mapstructure:"grpcEndpoint"`
	// The subscriber's consumer ID. Provided by the runtime.
	ConsumerID string `mapstructure:"consumerID" mdignore:"true"`
	// How messages are received: "stream" or "poll".
	ReceiveMode string `mapstructure:"receiveMode"`
	// How long a delivered message stays locked to this subscriber before it is redelivered.
	LockTTL time.Duration `mapstructure:"lockTTL"`
	// Stream mode: the most messages delivered to this instance and not yet settled.
	MaxActiveMessages int `mapstructure:"maxActiveMessages"`
	// Poll mode: how often the locks of messages taken but not yet settled are extended; 0 disables renewal.
	LockRenewalInterval time.Duration `mapstructure:"lockRenewalInterval"`
	// Poll mode: the most messages taken per dequeue.
	DequeueCount int `mapstructure:"dequeueCount"`
	// Poll mode: how long to wait before polling again when the queue is empty or locked.
	PollInterval time.Duration `mapstructure:"pollInterval"`
	// Let each subscriber instance hold its own locks.
	AllowCompetingConsumers bool `mapstructure:"allowCompetingConsumers"`
	// Call the handler for a dequeued batch one at a time ("single") or concurrently ("parallel").
	ConcurrencyMode pubsub.ConcurrencyMode `mapstructure:"concurrencyMode"`
	// The most handlers running at once in parallel mode; 0 is unlimited.
	MaxConcurrentHandlers int `mapstructure:"maxConcurrentHandlers"`
	// The most failed messages returned for redelivery per second; 0 is unlimited.
	MaxRetriableErrorsPerSec int `mapstructure:"maxRetriableErrorsPerSec"`
}

// ParseMetadata parses and validates the metadata; requireConsumerID makes consumerID required.
func ParseMetadata(props map[string]string, requireConsumerID bool) (Metadata, error) {
	md := Metadata{
		ReceiveMode:              ReceiveModeStream,
		LockTTL:                  30 * time.Second,
		MaxActiveMessages:        100,
		LockRenewalInterval:      10 * time.Second,
		DequeueCount:             10,
		PollInterval:             time.Second,
		AllowCompetingConsumers:  true,
		MaxRetriableErrorsPerSec: 10,
	}
	if err := kitmd.DecodeMetadata(props, &md); err != nil {
		return md, err
	}

	mode, err := pubsub.Concurrency(props)
	if err != nil {
		return md, err
	}
	md.ConcurrencyMode = mode

	switch {
	case md.HTTPEndpoint == "":
		return md, errors.New("httpEndpoint is required")
	case md.GRPCEndpoint == "":
		return md, errors.New("grpcEndpoint is required")
	case requireConsumerID && md.ConsumerID == "":
		return md, errors.New("consumerID is required")
	case md.ReceiveMode != ReceiveModeStream && md.ReceiveMode != ReceiveModePoll:
		return md, errors.New(`receiveMode must be "stream" or "poll"`)
	case md.MaxActiveMessages < 1 || md.MaxActiveMessages > 1000:
		return md, errors.New("maxActiveMessages must be between 1 and 1000")
	case md.MaxRetriableErrorsPerSec < 0:
		return md, errors.New("maxRetriableErrorsPerSec must be 0 (unlimited) or more")
	case md.LockTTL < time.Second:
		return md, errors.New("lockTTL must be at least 1s")
	case md.LockRenewalInterval != 0 && (md.LockRenewalInterval < time.Second || md.LockRenewalInterval >= md.LockTTL):
		return md, errors.New("lockRenewalInterval must be 0 (disabled), or at least 1s and less than lockTTL")
	case md.DequeueCount < 1 || md.DequeueCount > 1000:
		return md, errors.New("dequeueCount must be between 1 and 1000")
	case md.PollInterval <= 0:
		return md, errors.New("pollInterval must be greater than 0")
	case md.MaxConcurrentHandlers < 0:
		return md, errors.New("maxConcurrentHandlers must be 0 (unlimited) or more")
	}
	return md, nil
}
