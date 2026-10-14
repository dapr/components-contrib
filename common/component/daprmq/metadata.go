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
	// The defaults for every subscription; a subscription's metadata overrides them.
	SubscriptionOptions `mapstructure:",squash"`
}

// SubscriptionOptions is how a subscription consumes its queue. The component's metadata sets the
// defaults and each subscription's metadata may override them.
type SubscriptionOptions struct {
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
	md := Metadata{SubscriptionOptions: SubscriptionOptions{
		ReceiveMode:              ReceiveModeStream,
		LockTTL:                  30 * time.Second,
		MaxActiveMessages:        100,
		LockRenewalInterval:      10 * time.Second,
		DequeueCount:             10,
		PollInterval:             time.Second,
		AllowCompetingConsumers:  true,
		ConcurrencyMode:          pubsub.Parallel,
		MaxRetriableErrorsPerSec: 10,
	}}
	if err := kitmd.DecodeMetadata(props, &md); err != nil {
		return md, err
	}

	switch {
	case md.HTTPEndpoint == "":
		return md, errors.New("httpEndpoint is required")
	case md.GRPCEndpoint == "":
		return md, errors.New("grpcEndpoint is required")
	case requireConsumerID && md.ConsumerID == "":
		return md, errors.New("consumerID is required")
	}
	return md, md.SubscriptionOptions.parse(props)
}

// ParseSubscription returns the defaults overridden by a subscription's metadata, validated.
func ParseSubscription(defaults SubscriptionOptions, props map[string]string) (SubscriptionOptions, error) {
	opts := defaults
	if err := kitmd.DecodeMetadata(props, &opts); err != nil {
		return opts, err
	}
	return opts, opts.parse(props)
}

// parse sets the concurrency mode when props has one, then validates the options.
func (o *SubscriptionOptions) parse(props map[string]string) error {
	if _, ok := props[pubsub.ConcurrencyKey]; ok {
		mode, err := pubsub.Concurrency(props)
		if err != nil {
			return err
		}
		o.ConcurrencyMode = mode
	}

	switch {
	case o.ReceiveMode != ReceiveModeStream && o.ReceiveMode != ReceiveModePoll:
		return errors.New(`receiveMode must be "stream" or "poll"`)
	case o.MaxActiveMessages < 1 || o.MaxActiveMessages > 1000:
		return errors.New("maxActiveMessages must be between 1 and 1000")
	case o.MaxRetriableErrorsPerSec < 0:
		return errors.New("maxRetriableErrorsPerSec must be 0 (unlimited) or more")
	case o.LockTTL < time.Second:
		return errors.New("lockTTL must be at least 1s")
	case o.LockRenewalInterval != 0 && (o.LockRenewalInterval < time.Second || o.LockRenewalInterval >= o.LockTTL):
		return errors.New("lockRenewalInterval must be 0 (disabled), or at least 1s and less than lockTTL")
	case o.DequeueCount < 1 || o.DequeueCount > 1000:
		return errors.New("dequeueCount must be between 1 and 1000")
	case o.PollInterval <= 0:
		return errors.New("pollInterval must be greater than 0")
	case o.MaxConcurrentHandlers < 0:
		return errors.New("maxConcurrentHandlers must be 0 (unlimited) or more")
	}
	return nil
}
