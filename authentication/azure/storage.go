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

package azure

import (
	"fmt"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure"

	"github.com/dapr/kit/logger"
)

const (
	storageAccountKeyKey = "accountKey"
)

// GetAzureStorageCredentials returns a azblob.Credential object that can be used to authenticate an Azure Blob Storage SDK pipeline.
// First it tries to authenticate using shared key credentials (using an account key) if present. It falls back to attempting to use Azure AD (via a service principal or MSI).
func GetAzureStorageCredentials(log logger.Logger, accountName string, metadata map[string]string) (azblob.Credential, *azure.Environment, error) {
	settings, err := NewEnvironmentSettings("storage", metadata)
	if err != nil {
		return nil, nil, err
	}

	// Try using shared key credentials first
	accountKey, ok := metadata[storageAccountKeyKey]
	if ok && accountKey != "" {
		credential, newSharedKeyErr := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid credentials with error: %s", newSharedKeyErr.Error())
		}

		return credential, settings.AzureEnvironment, nil
	}

	// Fallback to using Azure AD
	spt, err := settings.GetServicePrincipalToken()
	if err != nil {
		return nil, nil, err
	}
	var tokenRefresher azblob.TokenRefresher = func(credential azblob.TokenCredential) time.Duration {
		log.Debug("Refreshing Azure Storage auth token")
		err := spt.Refresh()
		if err != nil {
			panic(err)
		}
		token := spt.Token()
		credential.SetToken(token.AccessToken)

		// Make the token expire 2 minutes earlier to get some extra buffer
		exp := token.Expires().Sub(time.Now().Add(2 * time.Minute))
		log.Debug("Received new token, valid for", exp)

		return exp
	}
	credential := azblob.NewTokenCredential("", tokenRefresher)

	return credential, settings.AzureEnvironment, nil
}
