// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

const (
	// FeatureETag is the feature to etag metadata in state store.
	FeatureETag Feature = "ETAG"
	// FeatureTransactional is the feature that performs transactional operations.
	FeatureTransactional Feature = "TRANSACTIONAL"
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
