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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

func TestNewFake(t *testing.T) {
	store := NewFake(logger.NewLogger("test"))
	require.NotNil(t, store)

	var _ binarystore.BinaryStore = store
}

func TestFakeRoundTrip(t *testing.T) {
	store := NewFake(logger.NewLogger("test"))
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
