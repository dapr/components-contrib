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

package secretstores

// SecretStore is the interface for a component that handles secrets management.
type SecretStore interface {
	// Init authenticates with the actual secret store and performs other init operation
	Init(metadata Metadata) error
	// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values
	GetSecret(req GetSecretRequest) (GetSecretResponse, error)
	// BulkGetSecrets retrieves all secrets in the store and returns a map of decrypted string/string values
	BulkGetSecret(req BulkGetSecretRequest) (BulkGetSecretResponse, error)
}
