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

package inmemory

import (
	"bytes"
	"errors"
	"io"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

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
	return newTestStoreWithProperties(t, props)
}

func newTestStoreWithProperties(t *testing.T, props map[string]string) *InMemoryBinaryStore {
	t.Helper()

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
		assert.Equal(t, defaultMaxSize, store.maxSize)
		assert.Equal(t, defaultMaxTotalSize, store.maxTotalSize)
	})

	t.Run("metadata is matched case-insensitively", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{
				"PREFIX":  "tenant-a",
				"MAXSIZE": "1024",
			}},
		}))
		assert.Equal(t, "tenant-a", store.prefix)
		assert.Equal(t, int64(1024), store.maxSize)
	})

	t.Run("sizes accept byte size quantities", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{
				"maxSize":      "1Ki",
				"maxTotalSize": "2Mi",
			}},
		}))
		assert.Equal(t, int64(1024), store.maxSize)
		assert.Equal(t, int64(2*1024*1024), store.maxTotalSize)
	})

	t.Run("maxSize must be positive", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{"maxSize": "0"}},
		})
		require.EqualError(t, err, "maxSize must be greater than zero")
	})

	t.Run("maxSize must not overflow when detecting oversized files", func(t *testing.T) {
		// Set reads one byte past maxSize to detect oversized files. ByteSize
		// is backed by resource.Quantity, which rejects values approaching
		// math.MaxInt64, so maxSize+1 cannot overflow int64.
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{"maxSize": strconv.FormatInt(math.MaxInt64, 10)}},
		})
		require.ErrorContains(t, err, "invalid value for maxSize")
	})

	t.Run("maxTotalSize must be positive", func(t *testing.T) {
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{"maxTotalSize": "0"}},
		})
		require.EqualError(t, err, "maxTotalSize must be greater than zero")
	})

	t.Run("maxTotalSize and maxSize combined must not overflow", func(t *testing.T) {
		// The capacity check adds up to maxSize bytes to totalBytes; values
		// large enough to overflow that sum are rejected while parsing.
		store := NewInMemoryBinaryStore(logger.NewLogger("test")).(*InMemoryBinaryStore)
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{
				"maxSize":      "1024",
				"maxTotalSize": strconv.FormatInt(math.MaxInt64, 10),
			}},
		})
		require.ErrorContains(t, err, "invalid value for maxTotalSize")
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(t, "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "prefix")
	assert.Contains(t, md, "maxSize")
	assert.Contains(t, md, "maxTotalSize")
}

func TestSetMaxSize(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{"maxSize": "7"})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "allowed.bin",
		Data:     bytes.NewBufferString("payload"),
	}))

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "large.bin",
		Data:     bytes.NewBufferString("too large"),
	})
	require.ErrorIs(t, err, binarystore.ErrFileTooLarge)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "large.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestMaxTotalSize(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{
		"maxSize":      "7",
		"maxTotalSize": "14",
	})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "a.bin",
		Data:     bytes.NewBufferString("1234567"),
	}))
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "b.bin",
		Data:     bytes.NewBufferString("1234567"),
	}))

	// A third distinct file would exceed the total capacity.
	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "c.bin",
		Data:     bytes.NewBufferString("1234567"),
	})
	require.ErrorIs(t, err, binarystore.ErrStoreFull)

	// Overwriting a file while the store is full succeeds in place: the
	// reservation credits the size of the file being replaced, which is
	// dropped when the upload commits.
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "a.bin",
		Data:      bytes.NewBufferString("7654321"),
		Overwrite: true,
	}))

	// Deleting frees capacity for new files.
	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "a.bin"}))
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "c.bin",
		Data:     bytes.NewBufferString("1234567"),
	}))
}

func TestMaxTotalSizeOverwriteGrowth(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{
		"maxSize":      "7",
		"maxTotalSize": "10",
	})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "a.bin",
		Data:     bytes.NewBufferString("1234567"),
	}))
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "b.bin",
		Data:     bytes.NewBufferString("abc"),
	}))

	// Replacing the 3-byte file with a 7-byte body would raise the total to
	// 14, beyond the 10-byte capacity, so it is rejected.
	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "b.bin",
		Data:      bytes.NewBufferString("7654321"),
		Overwrite: true,
	})
	require.ErrorIs(t, err, binarystore.ErrStoreFull)

	// The rejected overwrite leaves the original content intact.
	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "b.bin"})
	require.NoError(t, err)
	defer resp.Data.Close()
	got, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), got)

	// Replacing it with a body of the same size succeeds in place.
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "b.bin",
		Data:      bytes.NewBufferString("xyz"),
		Overwrite: true,
	}))
}

func TestSetReservesInFlightBytes(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{
		"maxSize":      "7",
		"maxTotalSize": "14",
	})

	gate := make(chan struct{})
	startUpload := func(name string) <-chan error {
		errCh := make(chan error, 1)
		go func() {
			errCh <- store.Set(t.Context(), &binarystore.SetRequest{
				FileName: name,
				Data:     &gatedReader{chunks: [][]byte{[]byte("1234567")}, gate: gate},
			})
		}()
		return errCh
	}

	// Two uploads together fill the store while still in flight.
	aErr := startUpload("a.bin")
	bErr := startUpload("b.bin")
	require.Eventually(t, func() bool {
		store.mu.RLock()
		defer store.mu.RUnlock()
		return store.inFlightBytes == 14
	}, 5*time.Second, time.Millisecond, "both in-flight uploads should have reserved their bytes")

	// A third upload must be rejected even though neither has committed yet.
	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "c.bin",
		Data:     bytes.NewBufferString("1234567"),
	})
	require.ErrorIs(t, err, binarystore.ErrStoreFull)

	// Releasing the gate lets both uploads commit within the cap.
	close(gate)
	require.NoError(t, <-aErr)
	require.NoError(t, <-bErr)

	store.mu.RLock()
	defer store.mu.RUnlock()
	assert.Equal(t, int64(14), store.totalBytes)
	assert.Zero(t, store.inFlightBytes)
}

// gatedReader delivers its chunks and then blocks until the gate is closed,
// so a test can hold an upload in flight.
type gatedReader struct {
	chunks [][]byte
	gate   <-chan struct{}
	i      int
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if g.i < len(g.chunks) {
		n := copy(p, g.chunks[g.i])
		g.i++
		return n, nil
	}
	<-g.gate
	return 0, io.EOF
}

func TestSetRejectsWithoutReadingData(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "exists.bin",
		Data:     bytes.NewBufferString("payload"),
	}))

	t.Run("invalid file name", func(t *testing.T) {
		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "/leading-slash",
			Data:     &unreadableReader{},
		})
		require.ErrorIs(t, err, binarystore.ErrInvalidFileName)
	})

	t.Run("existing file without overwrite", func(t *testing.T) {
		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "exists.bin",
			Data:     &unreadableReader{},
		})
		require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
	})
}

func TestSetNilDataStoresEmptyFile(t *testing.T) {
	store := newTestStore(t, "")

	// A request without Data must not panic; it stores an empty file.
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{FileName: "empty.bin"}))

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "empty.bin"})
	require.NoError(t, err)
	defer resp.Data.Close()
	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Empty(t, data)
}

var errMustNotRead = errors.New("data must not be read")

// unreadableReader fails on any read, proving that requests rejected before
// the body is consumed never pay for the read.
type unreadableReader struct{}

func (*unreadableReader) Read([]byte) (int, error) {
	return 0, errMustNotRead
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
