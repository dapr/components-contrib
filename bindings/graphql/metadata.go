//go:generate make -f ../../Makefile component-metadata-manifest type=bindings status=alpha version=v1 direction=output origin=$PWD "title=GraphQL"

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

package graphql

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &graphqlMetadata{}

type graphqlMetadata struct {
	// GraphQL endpoint string
	Endpoint string `json:"endpoint" mapstructure:"endpoint"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (b *graphqlMetadata) Defaults() any {
	return graphqlMetadata{}
}

// Note: we do not include any mdignored field.
func (b *graphqlMetadata) Examples() any {
	return graphqlMetadata{
		Endpoint: "http://localhost:4000/graphql/graphql",
	}
}

func (b *graphqlMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "query",
				Description: "Perform a query",
			},
			{
				Name:        "mutation",
				Description: "Perform a mutation",
			},
		},
	}
}
