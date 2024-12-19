//go:generate make -f ../../../Makefile component-metadata-manifest type=bindings builtinAuth="azuread" status=stable version=v1 direction=output origin=$PWD "title=Azure Cosmos DB (SQL API)"

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

package cosmosdb

import (
	"github.com/dapr/components-contrib/build-tools/pkg/metadataschema"
	"github.com/dapr/components-contrib/common/component"
)

// implement MetadataBuilder so each component will be properly parsed for the ast to auto-generate metadata manifest.
var _ component.MetadataBuilder = &cosmosdbMetadata{}

type cosmosdbMetadata struct {
	// The Cosmos DB url.
	URL string `json:"url"`
	// The key to authenticate to the Cosmos DB account.
	MasterKey string `json:"masterKey" authenticationProfile:"masterKey"`
	// The name of the database.
	Database string `json:"database"`
	//  The name of the collection (container).
	Collection string `json:"collection"`
	// The name of the key to extract from the payload (document to be created) that is used as the partition key. This name must match the partition key specified upon creation of the Cosmos DB container.
	PartitionKey string `json:"partitionKey"`
}

// Set the default values here.
// This unifies the setup across all components,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func (c *cosmosdbMetadata) Defaults() any {
	return cosmosdbMetadata{}
}

// Note: we do not include any mdignored field.
func (c *cosmosdbMetadata) Examples() any {
	return cosmosdbMetadata{
		URL:          "https://******.documents.azure.com:443/",
		MasterKey:    "my-secret-key",
		Database:     "OrdersDB",
		Collection:   "Orders",
		PartitionKey: "OrderId",
	}
}

func (c *cosmosdbMetadata) Binding() metadataschema.Binding {
	return metadataschema.Binding{
		Input:  true,
		Output: true,
		Operations: []metadataschema.BindingOperation{
			{
				Name:        "create",
				Description: "Create an item",
			},
		},
	}
}
