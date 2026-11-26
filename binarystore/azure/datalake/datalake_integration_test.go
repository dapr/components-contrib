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

package datalake

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func TestStoreBehaviourWithFloci(t *testing.T) {
	connectionString := os.Getenv("AZURE_DATALAKE_CONNECTION_STRING")
	require.NotEmpty(t, connectionString, "the integration test must target an ADLS Gen2 emulator")

	// Floci 0.16 returns 202 for DFS deletes, while the pinned Azure Go SDK
	// accepts 200. Delete remains covered by unit and cloud conformance tests.
	t.Run("round trips data with a prefix", func(t *testing.T) {
		store := newFlociStore(t, connectionString, "conf-prefix/nested")
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
	})

	t.Run("overwrites an existing file", func(t *testing.T) {
		store := newFlociStore(t, connectionString, "")
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("original")),
			Overwrite: false,
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

	t.Run("rejects a create-only write to an existing file", func(t *testing.T) {
		store := newFlociStore(t, connectionString, "")
		request := &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("original")),
			Overwrite: false,
		}
		require.NoError(t, store.Set(t.Context(), request))

		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      bytes.NewReader([]byte("replacement")),
			Overwrite: false,
		})
		require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
	})

	t.Run("returns not found for a missing file", func(t *testing.T) {
		store := newFlociStore(t, connectionString, "")
		_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "missing.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})
}

func newFlociStore(t *testing.T, connectionString, prefix string) binarystore.BinaryStore {
	t.Helper()

	store := NewAzureDataLakeStorage(logger.NewLogger("test"))
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
		Base: contribMetadata.Base{
			Properties: map[string]string{
				"connectionString": connectionString,
				"fileSystemName":   "dapr-binarystore-" + uuid.NewString(),
				"prefix":           prefix,
			},
		},
	}))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}
