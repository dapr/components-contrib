/*
Copyright 2025 The Dapr Authors
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

// newTestStore returns an uninitialised AzureDataLakeStorage cast to its
// concrete type so that unit tests can call unexported helpers without a live
// Azure connection.
func newTestStore() *AzureDataLakeStorage {
	return NewAzureDataLakeStorage(logger.NewLogger("test")).(*AzureDataLakeStorage)
}

// --- interface compliance ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = NewAzureDataLakeStorage(logger.NewLogger("test"))
}

// --- constructor ---

func TestNewAzureDataLakeStorage(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAzureDataLakeStorage(log)
	require.NotNil(t, store)
}

// --- Close ---

func TestClose(t *testing.T) {
	store := newTestStore()
	require.NoError(t, store.Close())
}

// --- Features ---

func TestFeatures(t *testing.T) {
	store := newTestStore()
	features := store.Features()
	// Features must return a non-nil slice (may be empty for this release).
	require.NotNil(t, features)
	assert.Empty(t, features)
}

// --- Set validation (no Azure connection required) ---

func TestSet_MissingFileName(t *testing.T) {
	store := newTestStore()

	err := store.Set(t.Context(), &binarystore.SetRequest{
		Data:      strings.NewReader("payload"),
		Overwrite: true,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- Get validation (no Azure connection required) ---

func TestGet_MissingFileName(t *testing.T) {
	store := newTestStore()

	_, err := store.Get(t.Context(), &binarystore.GetRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- Delete validation (no Azure connection required) ---

func TestDelete_MissingFileName(t *testing.T) {
	store := newTestStore()

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- SetRequest semantics ---

func TestSetRequest_OverwriteDefaultsFalse(t *testing.T) {
	req := &binarystore.SetRequest{
		FileName: "test.bin",
		Data:     strings.NewReader("hello"),
	}
	assert.False(t, req.Overwrite, "zero value of Overwrite must be false (create-only semantics)")
}

func TestSetRequest_OverwriteCanBeSetTrue(t *testing.T) {
	req := &binarystore.SetRequest{
		FileName:  "test.bin",
		Data:      strings.NewReader("hello"),
		Overwrite: true,
	}
	assert.True(t, req.Overwrite)
}

// --- Sentinel error identity ---

func TestSentinelErrors(t *testing.T) {
	t.Run("ErrFileAlreadyExists wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrFileAlreadyExists, binarystore.ErrFileAlreadyExists)
	})

	t.Run("ErrFileNotFound wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrFileNotFound, binarystore.ErrFileNotFound)
	})

	t.Run("ErrMissingFileName wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrMissingFileName, binarystore.ErrMissingFileName)
	})

	t.Run("sentinel errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrFileNotFound)
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrMissingFileName)
		assert.NotEqual(t, binarystore.ErrFileNotFound, binarystore.ErrMissingFileName)
	})
}

// --- GetComponentMetadata ---

func TestGetComponentMetadata(t *testing.T) {
	store := newTestStore()
	md := store.GetComponentMetadata()
	// The metadata map must be non-nil and contain at least the common Azure
	// Data Lake Storage properties (FileSystemName is always required).
	require.NotNil(t, md)
	_, hasFileSystem := md["FileSystemName"]
	assert.True(t, hasFileSystem, "FileSystemName must appear in component metadata")
}
