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

package secrets

import (
	contribCrypto "github.com/dapr/components-contrib/crypto"
	"github.com/dapr/components-contrib/metadata"
)

type secretsMetadata struct {
	// Default namespace to retrieve secrets from.
	// If unset, the namespace must be specified for each key, as `namespace/secretName/key`.
	DefaultNamespace string `json:"defaultNamespace" mapstructure:"defaultNamespace"`

	// Path to a kubeconfig file.
	// If empty, uses the default values.
	KubeconfigPath string `json:"kubeconfigPath" mapstructure:"kubeconfigPath"`
}

func (m *secretsMetadata) InitWithMetadata(meta contribCrypto.Metadata) error {
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	return nil
}

// Reset the object
func (m *secretsMetadata) reset() {
	m.DefaultNamespace = ""
}
