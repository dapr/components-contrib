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

package objectstorage

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	ociobjectstorage "github.com/oracle/oci-go-sdk/v54/objectstorage"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	flociTenancy = "ocid1.tenancy.oc1..flocilocaltenancy0000000000000000000000000000000000000000"
	flociUser    = "ocid1.user.oc1..flocilocaluser0000000000000000000000000000000000000000000000"
)

func TestStoreBehaviourWithFloci(t *testing.T) {
	require.NotEmpty(t, os.Getenv("OCI_FLOCI_ENDPOINT"), "the integration test must target an OCI emulator")
	privateKey := testPrivateKey(t)

	// Floci OCI 0.6 does not enforce If-None-Match on PutObject. Empty
	// streaming uploads also remain in unit and cloud conformance coverage
	// because the pinned OCI SDK encodes them as a zero-part multipart upload.
	t.Run("round trips data with a prefix", func(t *testing.T) {
		store := newFlociStore(t, privateKey, "conf-prefix/nested")
		payload := []byte("hello from Floci")

		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader(payload),
			Overwrite: false,
		}))

		response, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
		require.NoError(t, err)
		actual, err := io.ReadAll(response.Data)
		require.NoError(t, err)
		require.NoError(t, response.Data.Close())
		require.Equal(t, payload, actual)

		client, ok := store.client.(*ociObjectStoreClient)
		require.True(t, ok)
		require.Equal(t, []string{"conf-prefix/nested/file.bin"}, objectNames(t, client))
	})

	t.Run("overwrites an existing object", func(t *testing.T) {
		store := newFlociStore(t, privateKey, "")
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("original")),
			Overwrite: true,
		}))
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("replacement")),
			Overwrite: true,
		}))

		response, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
		require.NoError(t, err)
		actual, err := io.ReadAll(response.Data)
		require.NoError(t, err)
		require.NoError(t, response.Data.Close())
		require.Equal(t, []byte("replacement"), actual)
	})

	t.Run("deletes an object", func(t *testing.T) {
		store := newFlociStore(t, privateKey, "")
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("payload")),
			Overwrite: true,
		}))
		require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))

		_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("returns not found for a missing object", func(t *testing.T) {
		store := newFlociStore(t, privateKey, "")
		_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "missing.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})
}

func newFlociStore(t *testing.T, privateKey, prefix string) *ObjectStorage {
	t.Helper()

	store := NewOCIObjectStorage(logger.NewLogger("test"))
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				bucketNameKey:  "dapr-binarystore-" + uuid.NewString(),
				compartmentKey: flociTenancy,
				regionKey:      "us-ashburn-1",
				userKey:        flociUser,
				fingerPrintKey: "00:00:00:00:00:00:00:00",
				privateKeyKey:  privateKey,
				tenancyKey:     flociTenancy,
				"namespace":    "floci-local",
				"prefix":       prefix,
			},
		},
	}))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	typedStore, ok := store.(*ObjectStorage)
	require.True(t, ok)
	return typedStore
}

func objectNames(t *testing.T, client *ociObjectStoreClient) []string {
	t.Helper()

	names := make([]string, 0)
	var start *string
	for {
		response, err := client.objectClient.ListObjects(t.Context(), ociobjectstorage.ListObjectsRequest{
			NamespaceName: &client.metadata.Namespace,
			BucketName:    &client.metadata.BucketName,
			Start:         start,
		})
		require.NoError(t, err)
		for _, object := range response.Objects {
			if object.Name != nil {
				names = append(names, *object.Name)
			}
		}
		if response.NextStartWith == nil || *response.NextStartWith == "" {
			return names
		}
		start = response.NextStartWith
	}
}
