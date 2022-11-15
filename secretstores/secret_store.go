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

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/health"
)

// SecretStore is the interface for a component that handles secrets management.
type SecretStore interface {
	// Init authenticates with the actual secret store and performs other init operation
	Init(metadata Metadata) error
	// GetSecret retrieves a secret using a key and returns a map of decrypted string/string values.
	GetSecret(ctx context.Context, req GetSecretRequest) (GetSecretResponse, error)
	// BulkGetSecret retrieves all secrets in the store and returns a map of decrypted string/string values.
	BulkGetSecret(ctx context.Context, req BulkGetSecretRequest) (BulkGetSecretResponse, error)
	// Features lists the features supported by the secret store.
	Features() []Feature
}

func Ping(secretStore SecretStore) error {
	// checks if this secretStore has the ping option then executes
	if secretStoreWithPing, ok := secretStore.(health.Pinger); ok {
		return secretStoreWithPing.Ping()
	} else {
		return fmt.Errorf("ping is not implemented by this secret store")
	}
}
