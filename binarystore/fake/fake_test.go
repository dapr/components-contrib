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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

func newInitializedFake(t *testing.T, props map[string]string) binarystore.BinaryStore {
	t.Helper()
	f := NewFake(logger.NewLogger("test"))
	require.NoError(t, f.Init(t.Context(), metadataWith(props)))
	return f
}

func metadataWith(props map[string]string) binarystore.Metadata {
	md := binarystore.Metadata{}
	md.Properties = props
	return md
}

func TestFakeSetGetDeleteRoundTrip(t *testing.T) {
	f := newInitializedFake(t, nil)
	ctx := t.Context()

	require.NoError(t, f.Set(ctx, &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("payload"),
	}))

	resp, err := f.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	require.NoError(t, resp.Data.Close())
	assert.Equal(t, "payload", string(data))

	require.NoError(t, f.Delete(ctx, &binarystore.DeleteRequest{FileName: "file.bin"}))
	_, err = f.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestFakeOverwriteSemantics(t *testing.T) {
	f := newInitializedFake(t, nil)
	ctx := t.Context()

	require.NoError(t, f.Set(ctx, &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("first"),
	}))

	// Create-only write against an existing file must conflict.
	err := f.Set(ctx, &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("second"),
	})
	require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)

	// Overwrite replaces the content.
	require.NoError(t, f.Set(ctx, &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      strings.NewReader("second"),
		Overwrite: true,
	}))

	resp, err := f.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	require.NoError(t, resp.Data.Close())
	assert.Equal(t, "second", string(data))
}

func TestFakeNotFoundAndValidation(t *testing.T) {
	f := newInitializedFake(t, nil)
	ctx := t.Context()

	_, err := f.Get(ctx, &binarystore.GetRequest{FileName: "missing.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)

	err = f.Delete(ctx, &binarystore.DeleteRequest{FileName: "missing.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)

	err = f.Set(ctx, &binarystore.SetRequest{Data: strings.NewReader("x")})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	_, err = f.Get(ctx, &binarystore.GetRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	err = f.Delete(ctx, &binarystore.DeleteRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

func TestFakePrefixIsApplied(t *testing.T) {
	ctx := t.Context()

	prefixed := newInitializedFake(t, map[string]string{"prefix": "tenant-a"})
	require.NoError(t, prefixed.Set(ctx, &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("payload"),
	}))

	// The same name must resolve through the prefix on Get and Delete.
	resp, err := prefixed.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	require.NoError(t, resp.Data.Close())

	// The stored key must include the prefix internally.
	impl := prefixed.(*Fake)
	impl.mu.RLock()
	_, hasPrefixedKey := impl.files["tenant-a/file.bin"]
	_, hasBareKey := impl.files["file.bin"]
	impl.mu.RUnlock()
	assert.True(t, hasPrefixedKey, "stored key must include the configured prefix")
	assert.False(t, hasBareKey, "bare key must not exist when a prefix is configured")

	require.NoError(t, prefixed.Delete(ctx, &binarystore.DeleteRequest{FileName: "file.bin"}))
	_, err = prefixed.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}
