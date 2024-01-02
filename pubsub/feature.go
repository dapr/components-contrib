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
	"golang.org/x/exp/slices"
)

const (
	// FeatureMessageTTL is the feature to handle message TTL.
	FeatureMessageTTL Feature = "MESSAGE_TTL"
	// FeatureSubscribeWildcards is the feature to allow subscribing to topics/queues using a wildcard.
	FeatureSubscribeWildcards Feature = "SUBSCRIBE_WILDCARDS"
	FeatureBulkPublish        Feature = "BULK_PUBSUB"
)

// Feature names a feature that can be implemented by PubSub components.
type Feature string

// IsPresent checks if a given feature is present in the list.
func (f Feature) IsPresent(features []Feature) bool {
	return slices.Contains(features, f)
}
