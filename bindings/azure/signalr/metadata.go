//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="azuread" status=alpha version=v1 direction=output origin=$PWD "title=Azure SignalR"

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

package signalr

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &signalrMetadata{}

// Metadata keys.
// Azure AD credentials are parsed separately and not listed here.
type signalrMetadata struct {
	// Endpoint of Azure SignalR; required if not included in the connectionString or if using Microsoft Entra ID.
	Endpoint string `json:"endpoint,omitempty" mapstructure:"endpoint"`
	// Access key.
	AccessKey string `json:"accessKey,omitempty" mapstructure:"accessKey"`
	// Defines the hub in which the message will be send. The hub can be dynamically defined as a metadata value when publishing to an output binding (key is “hub”).
	Hub string `json:"hub,omitempty" mapstructure:"hub"`
	// The Azure SignalR connection string.
	ConnectionString string `json:"connectionString" mapstructure:"connectionString"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (s *signalrMetadata) Defaults() any {
	return signalrMetadata{}
}

// Note: we do not include any mdignored field.
func (s *signalrMetadata) Examples() any {
	return signalrMetadata{
		Endpoint:         "https://<your-azure-signalr>.service.signalr.net",
		AccessKey:        "your-access-key",
		Hub:              "myhub",
		ConnectionString: "Endpoint=https://<your-azure-signalr>.service.signalr.net;AccessKey=<your-access-key>;Version=1.0;",
	}
}

func (s *signalrMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create object",
			},
		},
	}
}
