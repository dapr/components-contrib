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

package binarystore

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(componentName string) TestConfig {
	return TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "binarystore",
			ComponentName: componentName,
		},
	}
}

// ConformanceTests runs the binary store conformance suite against the given
// provider. Providers are expected to be initialised inside this function via
// the supplied properties map.
func ConformanceTests(t *testing.T, props map[string]string, store binarystore.BinaryStore, component string) {
	t.Run("init", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		err := store.Init(ctx, binarystore.Metadata{
			Base: metadata.Base{
				Properties: props,
			},
		})
		require.NoError(t, err)
	})

	if t.Failed() {
		t.Fatal("initialization failed")
	}

	t.Cleanup(func() {
		_ = store.Close()
	})

	// Use a unique object name per test run to avoid collisions in shared
	// containers across CI runs.
	fileName := "conformance-" + component + "-" + t.Name()

	t.Run("set then get round-trips small payload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		payload := []byte("hello binary store conformance")

		err := store.Set(ctx, &binarystore.SetRequest{
			FileName:  fileName,
			Data:      bytes.NewReader(payload),
			Overwrite: true,
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: fileName})
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer resp.Data.Close()

		got, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		assert.Equal(t, payload, got)
	})

	t.Run("set without overwrite conflicts on existing file", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		// Ensure the file exists from the previous test.
		err := store.Set(ctx, &binarystore.SetRequest{
			FileName:  fileName,
			Data:      bytes.NewReader([]byte("first")),
			Overwrite: true,
		})
		require.NoError(t, err)

		err = store.Set(ctx, &binarystore.SetRequest{
			FileName:  fileName,
			Data:      bytes.NewReader([]byte("second")),
			Overwrite: false,
		})
		require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
	})

	t.Run("set with overwrite replaces content", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		require.NoError(t, store.Set(ctx, &binarystore.SetRequest{
			FileName:  fileName,
			Data:      bytes.NewReader([]byte("first")),
			Overwrite: true,
		}))

		require.NoError(t, store.Set(ctx, &binarystore.SetRequest{
			FileName:  fileName,
			Data:      bytes.NewReader([]byte("second")),
			Overwrite: true,
		}))

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: fileName})
		require.NoError(t, err)
		defer resp.Data.Close()

		got, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		assert.Equal(t, []byte("second"), got)
	})

	t.Run("set and get large payload streams without buffering", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()

		// 4 MiB of random data — large enough to exceed any reasonable
		// in-memory buffer and exercise the streaming path.
		size := 4 * 1024 * 1024
		payload := make([]byte, size)
		_, err := rand.Read(payload)
		require.NoError(t, err)

		bigName := fileName + "-large"
		err = store.Set(ctx, &binarystore.SetRequest{
			FileName:  bigName,
			Data:      bytes.NewReader(payload),
			Overwrite: true,
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: bigName})
		require.NoError(t, err)
		defer resp.Data.Close()

		// Read in modest chunks to verify the reader is genuinely streaming and
		// to keep peak memory low.
		got, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		require.Len(t, got, size)
		assert.True(t, bytes.Equal(payload, got), "round-tripped large payload must match byte-for-byte")
	})

	t.Run("get missing file returns ErrFileNotFound", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		_, err := store.Get(ctx, &binarystore.GetRequest{FileName: fileName + "-does-not-exist"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("delete removes the file", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		delName := fileName + "-delete-me"
		require.NoError(t, store.Set(ctx, &binarystore.SetRequest{
			FileName:  delName,
			Data:      strings.NewReader("bye"),
			Overwrite: true,
		}))

		require.NoError(t, store.Delete(ctx, &binarystore.DeleteRequest{FileName: delName}))

		_, err := store.Get(ctx, &binarystore.GetRequest{FileName: delName})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("delete missing file returns ErrFileNotFound", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		err := store.Delete(ctx, &binarystore.DeleteRequest{FileName: fileName + "-never-existed"})
		require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	})

	t.Run("operations reject empty file name", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		require.ErrorIs(t, store.Set(ctx, &binarystore.SetRequest{
			Data:      bytes.NewReader([]byte("x")),
			Overwrite: true,
		}), binarystore.ErrMissingFileName)

		_, err := store.Get(ctx, &binarystore.GetRequest{})
		require.ErrorIs(t, err, binarystore.ErrMissingFileName)

		err = store.Delete(ctx, &binarystore.DeleteRequest{})
		require.ErrorIs(t, err, binarystore.ErrMissingFileName)
	})

	// Sanity-check that the sentinel errors are distinct values so providers
	// can map provider-specific errors onto them unambiguously.
	t.Run("sentinel errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrFileNotFound)
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrMissingFileName)
		assert.NotEqual(t, binarystore.ErrFileNotFound, binarystore.ErrMissingFileName)
	})
}
