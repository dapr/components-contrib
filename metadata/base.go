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

package metadata

import (
	"strings"
)

// Base is the common metadata across components.
// All components-specific metadata should embed this.
type Base struct {
	// Name is the name of the component that's being used.
	Name string
	// Properties is the metadata properties.
	Properties map[string]string `json:"properties,omitempty"`
}

// GetProperty returns the value of a property, looking it up case-insensitively
func (b Base) GetProperty(names ...string) (string, bool) {
	// Note that we must look for "names" inside the map, and not vice-versa: this way we can guarantee the order
	// Start by lowercasing all metadata keys
	mdkeys := make(map[string]string, len(b.Properties))
	for k := range b.Properties {
		mdkeys[strings.ToLower(k)] = k
	}

	for _, k := range names {
		mapK := mdkeys[strings.ToLower(k)]
		if mapK != "" {
			return b.Properties[mapK], true
		}
	}

	return "", false
}
