/*
Copyright 2022 The Dapr Authors
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

package secretstores

import (
	"github.com/dapr/components-contrib/common/features"
)

// Feature names a feature that can be implemented by Secret Store components.
type Feature = features.Feature[SecretStore]

const (
	// FeatureMultipleKeyValuesPerSecret advertises that this SecretStore supports multiple keys-values under a single secret.
	FeatureMultipleKeyValuesPerSecret Feature = "MULTIPLE_KEY_VALUES_PER_SECRET"
)
