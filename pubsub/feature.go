/*
Copyright 2021 The Dapr Authors
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

package pubsub

import (
	"github.com/dapr/components-contrib/common/features"
)

const (
	// FeatureMessageTTL is the feature to handle message TTL.
	FeatureMessageTTL Feature = "MESSAGE_TTL"
	// FeatureSubscribeWildcards is the feature to allow subscribing to topics/queues using a wildcard.
	FeatureSubscribeWildcards Feature = "SUBSCRIBE_WILDCARDS"
	FeatureBulkPublish        Feature = "BULK_PUBSUB"
	// FeatureBulkSubscribeImmediate signals that the default bulk
	// subscriber should flush each delivery as soon as it arrives
	// rather than buffer until MaxMessagesCount is reached or
	// MaxAwaitDurationMs elapses.
	//
	// Concretely, the default bulk subscriber:
	//
	//   - Disables the MaxAwaitDurationMs timer entirely. There is
	//     no per-batch waiting period.
	//   - Still honours MaxMessagesCount as the upper bound on a
	//     single flush, and still coalesces messages that happen to
	//     arrive concurrently via a non-blocking channel drain. So
	//     "immediate" describes *when* a flush fires, not the batch
	//     size — bursts produce multi-entry batches up to
	//     MaxMessagesCount; quiet periods produce single-entry
	//     batches.
	//
	// Components should declare this feature when their delivery
	// model cannot satisfy a buffered batching window — typically
	// because the broker delivers serially and blocks on ack, so a
	// second message will never land in the buffer while the first
	// is pending. Without this feature such components produce
	// unbounded unacked-message buildup waiting for a batch that
	// can never form.
	//
	// This feature is only consulted by the default bulk-subscribe
	// wrapper. Components that natively implement BulkSubscriber
	// are unaffected — they own their own flushing policy.
	FeatureBulkSubscribeImmediate Feature = "BULK_SUBSCRIBE_IMMEDIATE"
)

// Feature names a feature that can be implemented by PubSub components.
type Feature = features.Feature[PubSub]
