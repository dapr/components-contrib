//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="azuread" status=alpha version=v1 "direction=input,output" origin=$PWD "title=Azure OpenAI"

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

package openai

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &openaiMetadata{}

type openaiMetadata struct {
	// APIKey is the API key for the Azure OpenAI API.
	APIKey string `json:"apiKey" mapstructure:"apiKey" authenticationProfile:"APIKey" binding:"output"`
	// Endpoint is the endpoint for the Azure OpenAI API.
	Endpoint string `json:"endpoint" mapstructure:"endpoint" binding:"output"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (o *openaiMetadata) Defaults() any {
	return openaiMetadata{}
}

// Note: we do not include any mdignored field.
func (o *openaiMetadata) Examples() any {
	return openaiMetadata{
		APIKey:   "1234567890abcdef",
		Endpoint: "https://myopenai.openai.azure.com",
	}
}

func (o *openaiMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "completion",
				Description: "Text completion",
			},
			{
				Name:        "chat-completion",
				Description: "Chat completion",
			},
			// TODO: below pls confirm this is correct bc docs showed it but original yaml manifest did not
			{
				Name:        "get-embedding",
				Description: "Get embedding",
			},
		},
	}
}
