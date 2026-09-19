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
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	mathrand "math/rand"
	"strconv"
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

func makeObjectName(component, suffix string) string {
	sanitized := strings.NewReplacer("/", "-", "\\", "-", " ", "-", "_", "-", ".", "-").Replace(component + "-" + suffix)
	var randomSuffix [8]byte
	if _, err := cryptorand.Read(randomSuffix[:]); err == nil {
		return sanitized + "-" + hex.EncodeToString(randomSuffix[:])
	}
	return sanitized + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func randReader(seed int64) io.Reader {
	return mathrand.New(mathrand.NewSource(seed)) //nolint:gosec // Deterministic reader for testing
}

// emptyReader is an io.Reader that always returns io.EOF immediately and is
// deliberately not one of the concrete types (*bytes.Buffer, *bytes.Reader,
// *strings.Reader, *os.File) that some SDKs special-case for zero-length
// detection. Requests arriving from the Dapr runtime typically use a reader
// of this shape, so this catches regressions where an empty payload fails to
// upload unless the underlying provider special-cases those specific types.
type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

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

	// Use a unique, slash-free object name per test run to avoid collisions in
	// shared containers across CI runs and nested path issues in ADLS.
	fileName := makeObjectName(component, t.Name())
	cleanupNames := map[string]struct{}{fileName: {}}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for name := range cleanupNames {
			if err := store.Delete(ctx, &binarystore.DeleteRequest{FileName: name}); err != nil && !errors.Is(err, binarystore.ErrFileNotFound) {
				t.Logf("cleanup delete %q failed: %v", name, err)
			}
		}
	})

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

	t.Run("set and get empty payload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		emptyName := makeObjectName(component, "empty")
		cleanupNames[emptyName] = struct{}{}

		err := store.Set(ctx, &binarystore.SetRequest{
			FileName:  emptyName,
			Data:      emptyReader{},
			Overwrite: true,
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: emptyName})
		require.NoError(t, err)
		require.NotNil(t, resp)
		defer resp.Data.Close()

		got, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		assert.Empty(t, got)
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

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: fileName})
		require.NoError(t, err)
		defer resp.Data.Close()

		got, err := io.ReadAll(resp.Data)
		require.NoError(t, err)
		assert.Equal(t, []byte("first"), got)
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

		size := 4 * 1024 * 1024
		seed := time.Now().UnixNano()
		payloadReader := io.LimitReader(randReader(seed), int64(size))
		bigName := makeObjectName(component, "large")
		cleanupNames[bigName] = struct{}{}

		err := store.Set(ctx, &binarystore.SetRequest{
			FileName:  bigName,
			Data:      payloadReader,
			Overwrite: true,
		})
		require.NoError(t, err)

		resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: bigName})
		require.NoError(t, err)
		defer resp.Data.Close()

		expected := sha256.New()
		_, err = io.Copy(expected, io.LimitReader(randReader(seed), int64(size)))
		require.NoError(t, err)

		gotHash := sha256.New()
		buf := make([]byte, 64*1024)
		readTotal := 0
		for {
			n, readErr := resp.Data.Read(buf)
			if n > 0 {
				_, err = gotHash.Write(buf[:n])
				require.NoError(t, err)
				readTotal += n
			}
			if readErr == io.EOF {
				break
			}
			require.NoError(t, readErr)
		}
		require.Equal(t, size, readTotal)
		assert.Equal(t, expected.Sum(nil), gotHash.Sum(nil), "round-tripped large payload must match byte-for-byte")
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
		cleanupNames[delName] = struct{}{}
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
