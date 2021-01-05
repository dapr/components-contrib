// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

const (
	// FeatureMessageTTL is the feature to handle message TTL.
	FeatureMessageTTL Feature = "MESSAGE_TTL"
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
