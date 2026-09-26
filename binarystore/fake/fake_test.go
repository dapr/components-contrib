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

package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
)

func TestNew(t *testing.T) {
	store := New()
	require.NotNil(t, store)
}

func TestFakeRoundTrip(t *testing.T) {
	store := New()
	t.Cleanup(func() {
		_ = store.Close()
	})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     bytes.NewReader([]byte("payload")),
	}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	defer resp.Data.Close()
	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data)

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))

	_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestFakeOverridesFailOnDemand(t *testing.T) {
	errBoom := errors.New("boom")

	store := New().
		WithSet(func(context.Context, *binarystore.SetRequest) error {
			return errBoom
		}).
		WithGet(func(context.Context, *binarystore.GetRequest) (*binarystore.GetResponse, error) {
			return nil, errBoom
		}).
		WithDelete(func(context.Context, *binarystore.DeleteRequest) error {
			return errBoom
		})

	require.ErrorIs(t, store.Set(t.Context(), &binarystore.SetRequest{FileName: "file.bin"}), errBoom)

	_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, errBoom)

	require.ErrorIs(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}), errBoom)
}

func TestFakeUnsetOverridesUseInMemoryBacking(t *testing.T) {
	// Only Delete fails; the remaining operations keep the in-memory behaviour.
	errBoom := errors.New("boom")

	store := New().WithDelete(func(context.Context, *binarystore.DeleteRequest) error {
		return errBoom
	})
	t.Cleanup(func() {
		_ = store.Close()
	})

	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{}))
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     bytes.NewReader([]byte("payload")),
	}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	defer resp.Data.Close()

	require.ErrorIs(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}), errBoom)
	assert.NotNil(t, store.GetComponentMetadata())
	assert.Empty(t, store.Features())
}
