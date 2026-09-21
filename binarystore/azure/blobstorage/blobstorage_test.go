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

package blobstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/blobstorage"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(client blobStoreClient, prefix string) *AzureBlobStorage {
	return &AzureBlobStorage{
		metadata: &storagecommon.BlobStorageMetadata{
			ContainerClientOpts: storagecommon.ContainerClientOpts{ContainerName: "test-container"},
			Prefix:              prefix,
		},
		client:   client,
		logger:   logger.NewLogger("test"),
	}
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		client := newFakeBlobClient()
		return storetest.Harness{
			Store:  newTestStore(client, prefix),
			Prefix: prefix,
			Names:  client.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*AzureBlobStorage)(nil)
}

func TestNewAzureBlobStorage(t *testing.T) {
	require.NotNil(t, NewAzureBlobStorage(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := NewAzureBlobStorage(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeBlobClient()
		require.NoError(t, newTestStore(client, "").Close())
		assert.True(t, client.closed)
	})
}

// --- provider specific behaviour ---

func TestSetOverwriteDoesNotMaskConditionErrors(t *testing.T) {
	// A ConditionNotMet raised while overwriting is not an existence conflict
	// and must not be reported as ErrFileAlreadyExists.
	client := newFakeBlobClient()
	client.putErr = &azcore.ResponseError{ErrorCode: string(bloberror.ConditionNotMet)}
	store := newTestStore(client, "")

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      bytes.NewReader([]byte("payload")),
		Overwrite: true,
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, binarystore.ErrFileAlreadyExists)
}

// --- metadata ---

func TestParseMetadata(t *testing.T) {
	t.Run("missing account name", func(t *testing.T) {
		store := NewAzureBlobStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{})
		require.Error(t, err)
	})

	t.Run("missing container name", func(t *testing.T) {
		store := NewAzureBlobStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{
				"accountName": "myaccount",
				"accountKey":  "a2V5",
			}},
		})
		require.Error(t, err)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(newFakeBlobClient(), "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "ContainerName")
	assert.Contains(t, md, "prefix")
}

// --- error classification ---

func TestErrorClassification(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		assert.True(t, isNotFound(&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}))
		assert.False(t, isNotFound(&azcore.ResponseError{ErrorCode: string(bloberror.ConditionNotMet)}))
		assert.False(t, isNotFound(errors.New("boom")))
	})

	t.Run("precondition failed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(bloberror.BlobAlreadyExists)}))
		assert.True(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(bloberror.ConditionNotMet)}))
		assert.False(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}))
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})
}

// --- fakes ---

type fakeBlobClient struct {
	objects map[string][]byte
	putErr  error
	closed  bool
}

func newFakeBlobClient() *fakeBlobClient {
	return &fakeBlobClient{objects: map[string][]byte{}}
}

func (f *fakeBlobClient) names() []string {
	names := make([]string, 0, len(f.objects))
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (f *fakeBlobClient) putObject(_ context.Context, name string, data io.Reader, overwrite bool) error {
	if f.putErr != nil {
		return f.putErr
	}
	if _, ok := f.objects[name]; ok && !overwrite {
		return &azcore.ResponseError{ErrorCode: string(bloberror.BlobAlreadyExists)}
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.objects[name] = b
	return nil
}

func (f *fakeBlobClient) getObject(_ context.Context, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeBlobClient) deleteObject(_ context.Context, name string) error {
	if _, ok := f.objects[name]; !ok {
		return &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
	}
	delete(f.objects, name)
	return nil
}

func (f *fakeBlobClient) close() error {
	f.closed = true
	return nil
}
