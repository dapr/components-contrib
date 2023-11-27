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
	wantKeys := make(map[string]struct{}, len(names))
	for _, v := range names {
		wantKeys[strings.ToLower(v)] = struct{}{}
	}

	for k, v := range b.Properties {
		_, ok := wantKeys[strings.ToLower(k)]
		if ok {
			return v, true
		}
	}

	return "", false
}
