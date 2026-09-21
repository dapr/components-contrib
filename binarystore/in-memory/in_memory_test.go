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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(t *testing.T, prefix string) *InMemoryBinaryStore {
	t.Helper()

	props := map[string]string{}
	if prefix != "" {
		props["prefix"] = prefix
	}

	store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
		Base: contribMetadata.Base{Properties: props},
	}))
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		store := newTestStore(t, prefix)
		return storetest.Harness{
			Store:  store,
			Prefix: prefix,
			Names:  store.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*InMemoryBinaryStore)(nil)
}

func TestNewInMemoryBinaryStore(t *testing.T) {
	require.NotNil(t, NewInMemoryBinaryStore(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("uninitialised store", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured store", func(t *testing.T) {
		store := newTestStore(t, "")
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "file.bin",
			Data:     bytes.NewReader([]byte("payload")),
		}))
		require.NoError(t, store.Close())
	})
}

// --- metadata ---

func TestParseMetadata(t *testing.T) {
	t.Run("no metadata", func(t *testing.T) {
		store := newTestStore(t, "")
		assert.Empty(t, store.prefix)
	})

	t.Run("prefix is matched case-insensitively", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{"PREFIX": "tenant-a"}},
		}))
		assert.Equal(t, "tenant-a", store.prefix)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(t, "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "prefix")
}

// names returns the stored object names, used by the shared behaviour suite to
// assert that the configured prefix is applied.
func (s *InMemoryBinaryStore) names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.files))
	for name := range s.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
