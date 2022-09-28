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

package metadataschema

import (
	"errors"
	"strings"
)

// IsValid performs additional validation and returns true if the object is valid.
func (c ComponentMetadata) IsValid() error {
	// Sanity check for bindings
	if c.Type == "" {
		return errors.New("type is empty")
	}
	if c.Type == "bindings" && c.Binding == nil {
		return errors.New("property binding is required for components of type 'bindings'")
	}
	if c.Type != "bindings" && c.Binding != nil {
		return errors.New("property binding is not allowed in components that are not of type 'bindings'")
	}

	// Ensure that there's a URL called Reference to the Dapr docs
	if len(c.URLs) < 1 {
		return errors.New("property urls must contain at least one URL to the Dapr docs with title 'Reference'")
	}
	hasReferenceUrl := false
	for _, u := range c.URLs {
		if u.Title == "Reference" && strings.HasPrefix(u.URL, "https://docs.dapr.io/reference/components-reference/") {
			hasReferenceUrl = true
		}
	}
	if !hasReferenceUrl {
		return errors.New("property urls must a link to the Dapr docs with title 'Reference' and URL starting with 'https://docs.dapr.io/reference/components-reference/'")
	}

	return nil
}
