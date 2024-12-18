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

package cosmosdbgremlinapi

type cosmosdbMetadata struct {
	// The Cosmos DB url.
	URL string `json:"url"`
	// The key to authenticate to the Cosmos DB account.
	APMasterKey string `json:"masterKey"` // AP prefix indicates auth profile metadata specifically
	// The username of the Cosmos DB database.
	APUsername string `json:"username"` // AP prefix indicates auth profile metadata specifically
}

// Set the default values here.
// This unifies the setup across all componets,
// and makes it easy for us to auto-generate the component metadata default values,
// while also leveraging the default values for types thanks to Go.
func Defaults() cosmosdbMetadata {
	return cosmosdbMetadata{}
}

// Note: we do not include any mdignored field.
func Examples() cosmosdbMetadata {
	return cosmosdbMetadata{
		URL:         "https://******.documents.azure.com:443/",
		APMasterKey: "my-secret-key",
		APUsername:  "/dbs/<database_name>/colls/<graph_name>",
	}
}
