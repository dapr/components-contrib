//go:generate make -f ../../../../Makefile component-metadata-manifest type=bindings builtinAuth="azuread" status=alpha version=v1 direction=output origin=$PWD "title=Azure Cosmos DB (Gremlin API)"

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

package gremlinapi

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &gremlinapiMetadata{}

type gremlinapiMetadata struct {
	// The Cosmos DB URL for Gremlin APIs.
	URL string `json:"url"`
	// The key to authenticate to the Cosmos DB account.
	MasterKey string `json:"masterKey" authenticationProfile:"masterKey"`
	// The username of the Cosmos DB database.
	Username string `json:"username" authenticationProfile:"masterKey"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (g *gremlinapiMetadata) Defaults() any {
	return gremlinapiMetadata{}
}

// Note: we do not include any mdignored field.
func (g *gremlinapiMetadata) Examples() any {
	return gremlinapiMetadata{
		URL:       "wss://******.gremlin.cosmos.azure.com:443/",
		MasterKey: "my-secret-key",
		Username:  "/dbs/<database_name>/colls/<graph_name>",
	}
}

func (g *gremlinapiMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  false,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "query",
				Description: "Perform a query",
			},
		},
	}
}
