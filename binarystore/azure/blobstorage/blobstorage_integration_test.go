//go:build integration_test
// +build integration_test

/*
Copyright 2026 The Dapr Authors
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

package blobstorage

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestStoreBehaviourWithFloci(t *testing.T) {
	connectionString := os.Getenv("AZURE_BLOB_CONNECTION_STRING")
	require.NotEmpty(t, connectionString, "the integration test must target a Blob Storage emulator")

	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		store := NewAzureBlobStorage(logger.NewLogger("test"))
		require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{
				Properties: map[string]string{
					"connectionString": connectionString,
					"containerName":    "dapr-binarystore-" + uuid.NewString(),
					"prefix":           prefix,
				},
			},
		}))
		t.Cleanup(func() {
			require.NoError(t, store.Close())
		})

		typedStore, ok := store.(*AzureBlobStorage)
		require.True(t, ok)
		client, ok := typedStore.client.(*azureBlobClient)
		require.True(t, ok)

		return storetest.Harness{
			Store:  store,
			Prefix: prefix,
			Names: func() []string {
				return blobNames(t, client)
			},
		}
	})
}

func blobNames(t *testing.T, client *azureBlobClient) []string {
	t.Helper()

	names := make([]string, 0)
	pager := client.containerClient.NewListBlobsFlatPager(nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				names = append(names, *item.Name)
			}
		}
	}
	return names
}
