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

package keyvault

import (
	"errors"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	contribCrypto "github.com/dapr/components-contrib/crypto"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
)

const defaultRequestTimeout = 30 * time.Second

type keyvaultMetadata struct {
	// Name of the Azure Key Vault resource (required).
	VaultName string `json:"vaultName" mapstructure:"vaultName"`

	// Timeout for network requests, as a Go duration string (e.g. "30s")
	// Defaults to "30s".
	RequestTimeout time.Duration `json:"requestTimeout" mapstructure:"requestTimeout"`

	// Internal properties
	vaultDNSSuffix string
	cred           azcore.TokenCredential
}

func (m *keyvaultMetadata) InitWithMetadata(meta contribCrypto.Metadata) error {
	m.reset()

	// Decode the metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	// Vault name
	if m.VaultName == "" {
		return errors.New("metadata property 'vaultName' is required")
	}

	// Set default requestTimeout if empty
	if m.RequestTimeout < time.Second {
		m.RequestTimeout = defaultRequestTimeout
	}

	// Get the DNS suffix
	settings, err := azauth.NewEnvironmentSettings(meta.Properties)
	if err != nil {
		return err
	}
	m.vaultDNSSuffix = settings.EndpointSuffix(azauth.ServiceAzureKeyVault)

	// Get the credentials object
	m.cred, err = settings.GetTokenCredential()
	if err != nil {
		return err
	}

	return nil
}

// Reset the object
func (m *keyvaultMetadata) reset() {
	m.VaultName = ""
	m.RequestTimeout = defaultRequestTimeout

	m.vaultDNSSuffix = ""
	m.cred = nil
}
