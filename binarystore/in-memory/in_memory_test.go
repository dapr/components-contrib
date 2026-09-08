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

package inmemory

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(t *testing.T) binarystore.BinaryStore {
	t.Helper()

	store := NewInMemoryBinaryStore(logger.NewLogger("test"))
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{}))
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func TestSetAndGetRoundTrip(t *testing.T) {
	store := newTestStore(t)

	payload := []byte("hello world")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader(payload),
		Overwrite: true,
	}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "myfile"})
	require.NoError(t, err)
	defer resp.Data.Close()

	got, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestSetWithoutOverwriteConflicts(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("first")),
		Overwrite: true,
	}))

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("second")),
		Overwrite: false,
	})
	require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
}

func TestSetWithOverwriteReplacesContent(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("first")),
		Overwrite: true,
	}))
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("second")),
		Overwrite: true,
	}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "myfile"})
	require.NoError(t, err)
	defer resp.Data.Close()

	got, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, []byte("second"), got)
}

func TestGetMissingFileReturnsNotFound(t *testing.T) {
	store := newTestStore(t)

	_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "does-not-exist"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestDeleteRemovesFile(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("bye")),
		Overwrite: true,
	}))
	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "myfile"}))

	_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "myfile"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestDeleteMissingFileReturnsNotFound(t *testing.T) {
	store := newTestStore(t)

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "never-existed"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestOperationsRejectEmptyFileName(t *testing.T) {
	store := newTestStore(t)

	err := store.Set(t.Context(), &binarystore.SetRequest{
		Data:      bytes.NewReader([]byte("x")),
		Overwrite: true,
	})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	err = store.Delete(t.Context(), &binarystore.DeleteRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

func TestPrefixIsAppliedToFileNames(t *testing.T) {
	store := NewInMemoryBinaryStore(logger.NewLogger("test"))
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{"prefix": "myprefix"},
		},
	}))
	t.Cleanup(func() {
		_ = store.Close()
	})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "myfile",
		Data:      bytes.NewReader([]byte("data")),
		Overwrite: true,
	}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "myfile"})
	require.NoError(t, err)
	defer resp.Data.Close()

	got, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got)
}
