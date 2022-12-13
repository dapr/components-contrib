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

package cfkv

import (
	"errors"
	"regexp"

	"github.com/dapr/components-contrib/internal/component/cloudflare/workers"
)

// Component metadata struct.
type componentMetadata struct {
	workers.BaseMetadata `mapstructure:",squash"`
	KVNamespaceName      string `mapstructure:"kvNamespaceName"`
	KVNamespaceID        string `mapstructure:"kvNamespaceID"`
}

var kvNamespaceValidation = regexp.MustCompile("^([a-zA-Z0-9_\\-\\.]+)$")

// Validate the metadata object.
func (m *componentMetadata) Validate() error {
	// Start by validating the base metadata, then validate the properties specific to this component
	err := m.BaseMetadata.Validate()
	if err != nil {
		return err
	}

	// KVNamespaceName
	if m.KVNamespaceName == "" {
		return errors.New("property 'kvNamespaceName' is required")
	}
	if !kvNamespaceValidation.MatchString(m.KVNamespaceName) {
		return errors.New("metadata property 'kvNamespaceName' is invalid")
	}

	// KVNamespaceID
	if m.KVNamespaceID == "" {
		return errors.New("property 'kvNamespaceID' is required")
	}
	if !kvNamespaceValidation.MatchString(m.KVNamespaceID) {
		return errors.New("metadata property 'kvNamespaceID' is invalid")
	}

	return nil
}
