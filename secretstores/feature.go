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

// Feature names a feature that can be implemented by Secret Store components.
type Feature string

const (
	// FeatureMultipleKeyValuesPerSecret advertises that this SecretStore supports multiple keys-values under a single secret.
	FeatureMultipleKeyValuesPerSecret Feature = "MULTIPLE_KEY_VALUES_PER_SECRET"
)

// IsPresent checks if a given feature is present in the list.
func (f Feature) IsPresent(features []Feature) bool {
	for _, feature := range features {
		if feature == f {
			return true
		}
	}

	return false
}
