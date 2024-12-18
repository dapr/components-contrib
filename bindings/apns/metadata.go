//go:generate make -f ../../Makefile component-metadata-manifest type=bindings status=stable version=v1 direction=output origin=$PWD "title=Apple Push Notification Service"

/*
Copyright 2024 The Dapr Authors
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

package apns

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &apnsMetadata{}

type apnsMetadata struct {
	// Tells the binding which APNs service to use. Set to "true" to use the development service or "false" to use the production service.
	Development bool `json:"development,omitempty" mapstructure:"development"`
	// The identifier for the private key from the Apple Developer Portal.
	KeyID string `json:"key-id" mapstructure:"key-id"`
	// The identifier for the organization or author from the Apple Developer Portal.
	TeamID string `json:"team-id" mapstructure:"team-id"`
	// A PKCS #8-formatted private key. It is intended that the private key is stored in the secret store and not exposed directly in the configuration.
	PrivateKey string `json:"privateKey" mapstructure:"private-key"`

	// TODO: docs need Development required set to false
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (a *apnsMetadata) Defaults() any {
	return apnsMetadata{
		Development: true,
	}
}

// Note: we do not include any mdignored field.
func (a *apnsMetadata) Examples() any {
	return apnsMetadata{
		Development: false,
		KeyID:       "private-key-id",
		TeamID:      "team-id",
		PrivateKey:  "pem file",
	}
}

func (a *apnsMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create notification",
			},
		},
	}
}
