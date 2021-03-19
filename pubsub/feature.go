// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

const (
	// FeatureMessageTTL is the feature to handle message TTL.
	FeatureMessageTTL Feature = "MESSAGE_TTL"
	// FeatureRawData is the feature not to use runtime_pubsub.NewCloudEvent but use Data from app.
	FeatureRawData Feature = "RAW_DATA"
)

// Feature names a feature that can be implemented by PubSub components.
type Feature string

// IsPresent checks if a given feature is present in the list.
func (f Feature) IsPresent(features []Feature) bool {
	for _, feature := range features {
		if feature == f {
			return true
		}
	}

	return false
}
