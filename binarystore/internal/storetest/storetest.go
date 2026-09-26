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

// Package storetest provides the shared unit-test suite that every
// binarystore implementation runs against a fake backend. Keeping the suite
// in one place means the providers are held to the same behaviour: identical
// sentinel errors, identical create-only and overwrite semantics, identical
// streaming and prefix handling.
//
// The conformance suite in tests/conformance/binarystore asserts the same
// behaviour against real backends; this suite is its offline counterpart.
package storetest

import (
	"bytes"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
)

// Harness is a single provider instance wired to a fake backend.
type Harness struct {
	// Store is an initialised binary store configured with Prefix.
	Store binarystore.BinaryStore

	// Prefix is the object prefix the store was configured with.
	Prefix string

	// Names returns the object names currently held by the backend, as the
	// backend sees them (so including Prefix). It may be nil if the backend
	// cannot enumerate its contents, in which case prefix assertions are
	// skipped.
	Names func() []string
}

// Factory builds a Harness for a store configured with the given prefix.
type Factory func(t *testing.T, prefix string) Harness

// EmptyReader is an io.Reader that immediately reports io.EOF.
//
// It is deliberately not one of the concrete types (*bytes.Buffer,
// *bytes.Reader, *strings.Reader, *os.File) that provider SDKs special-case
// when detecting zero-length bodies. Readers handed to a component by the
// Dapr runtime have this shape, so using it here catches providers that only
// support empty payloads for the special-cased types.
type EmptyReader struct{}

// Read implements io.Reader.
func (EmptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// RunSuite runs the shared binary store behaviour suite, once against a store
// without a prefix and once against a store configured with a prefix.
func RunSuite(t *testing.T, newHarness Factory) {
	t.Helper()

	t.Run("no prefix", func(t *testing.T) {
		runCases(t, newHarness, "")
	})

	t.Run("with prefix", func(t *testing.T) {
		runCases(t, newHarness, "conf-prefix/nested")
	})
}

func runCases(t *testing.T, newHarness Factory, prefix string) {
	t.Helper()

	t.Run("features are empty but non-nil", func(t *testing.T) {
		h := newHarness(t, prefix)
		features := h.Store.Features()
		require.NotNil(t, features)
		assert.Empty(t, features)
	})

	t.Run("operations reject empty file name", func(t *testing.T) {
		h := newHarness(t, prefix)

		err := h.Store.Set(t.Context(), &binarystore.SetRequest{
			Data:      strings.NewReader("payload"),
			Overwrite: true,
		})
		require.ErrorIs(t, err, binarystore.ErrMissingFileName)

		_, err = h.Store.Get(t.Context(), &binarystore.GetRequest{})
		require.ErrorIs(t, err, binarystore.ErrMissingFileName)

		err = h.Store.Delete(t.Context(), &binarystore.DeleteRequest{})
		require.ErrorIs(t, err, binarystore.ErrMissingFileName)
	})

	t.Run("set then get round-trips payload", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("payload"),
			Overwrite: true,
		}))

		assert.Equal(t, "payload", string(mustGet(t, h, "file.bin")))
	})

	t.Run("set without overwrite creates a new file", func(t *testing.T) {
		h := newHarness(t, prefix)

		// The create-only path carries each provider's server-side
		// precondition, so it must succeed when the name is free.
		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("first"),
			Overwrite: false,
		}))

		assert.Equal(t, "first", string(mustGet(t, h, "file.bin")))
	})

	t.Run("set without overwrite conflicts on existing file", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("first"),
			Overwrite: true,
		}))

		err := h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("second"),
			Overwrite: false,
		})
		require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)

		assert.Equal(t, "first", string(mustGet(t, h, "file.bin")), "a rejected create-only write must not modify the file")
	})

	t.Run("set with overwrite replaces content", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("first"),
			Overwrite: true,
		}))
		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("second"),
			Overwrite: true,
		}))

		assert.Equal(t, "second", string(mustGet(t, h, "file.bin")))
	})

	t.Run("set and get empty payload", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "empty.bin",
			Data:      EmptyReader{},
			Overwrite: true,
		}))

		assert.Empty(t, mustGet(t, h, "empty.bin"))
	})

	t.Run("set streams multi-part payloads", func(t *testing.T) {
		h := newHarness(t, prefix)

		// Larger than a single buffered read so that chunked upload paths are
		// exercised, and compared byte-for-byte on the way back out.
		payload := bytes.Repeat([]byte("dapr"), 256*1024)
		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "large.bin",
			Data:      bytes.NewReader(payload),
			Overwrite: true,
		}))

		assert.Equal(t, payload, mustGet(t, h, "large.bin"))
	})

	t.Run("get returns a closable streaming body", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("payload"),
			Overwrite: true,
		}))

		resp, err := h.Store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Data)

		data, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		assert.Equal(t, "payload", string(data))
		require.NoError(t, resp.Data.Close())
	})

	t.Run("get missing file returns ErrFileNotFound", func(t *testing.T) {
		h := newHarness(t, prefix)

		_, err := h.Store.Get(t.Context(), &binarystore.GetRequest{FileName: "missing.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("delete removes the file", func(t *testing.T) {
		h := newHarness(t, prefix)

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("payload"),
			Overwrite: true,
		}))
		require.NoError(t, h.Store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))

		_, err := h.Store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("delete missing file returns ErrFileNotFound", func(t *testing.T) {
		h := newHarness(t, prefix)

		err := h.Store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "missing.bin"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("prefix is applied to stored object names", func(t *testing.T) {
		h := newHarness(t, prefix)
		if h.Names == nil {
			t.Skip("backend cannot enumerate object names")
		}

		require.NoError(t, h.Store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("payload"),
			Overwrite: true,
		}))

		names := h.Names()
		sort.Strings(names)
		require.Equal(t, []string{binarystore.ObjectPath(h.Prefix, "file.bin")}, names)

		// The prefix is an implementation detail of the backend layout: the
		// caller always refers to the file by its unprefixed name.
		require.NoError(t, h.Store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))
		assert.Empty(t, h.Names())
	})

	t.Run("close succeeds", func(t *testing.T) {
		h := newHarness(t, prefix)
		require.NoError(t, h.Store.Close())
	})
}

func mustGet(t *testing.T, h Harness, fileName string) []byte {
	t.Helper()

	resp, err := h.Store.Get(t.Context(), &binarystore.GetRequest{FileName: fileName})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)
	t.Cleanup(func() {
		_ = resp.Data.Close()
	})

	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	return data
}
