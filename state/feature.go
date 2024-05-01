/*
Copyright 2023 The Dapr Authors
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

package state

import (
	"github.com/dapr/components-contrib/common/features"
)

const (
	// FeatureETag is the feature to etag metadata in state store.
	FeatureETag Feature = "ETAG"
	// FeatureTransactional is the feature that performs transactional operations.
	FeatureTransactional Feature = "TRANSACTIONAL"
	// FeatureQueryAPI is the feature that performs query operations.
	FeatureQueryAPI Feature = "QUERY_API"
	// FeatureTTL is the feature that supports TTLs.
	FeatureTTL Feature = "TTL"
	// FeatureDeleteWithPrefix is the feature that supports deleting with prefix.
	FeatureDeleteWithPrefix Feature = "DELETE_WITH_PREFIX"
	// FeaturePartitionKey is the feature that supports the partition
	FeaturePartitionKey Feature = "PARTITION_KEY"
)

// Feature names a feature that can be implemented by state store components.
type Feature = features.Feature[BaseStore]
