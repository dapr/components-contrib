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

package datalake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/datalake"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(client datalakeStoreClient, prefix string) *AzureDataLakeStorage {
	return &AzureDataLakeStorage{
		metadata: &storagecommon.DataLakeMetadata{
			FileSystemClientOpts: storagecommon.FileSystemClientOpts{FileSystemName: "test-filesystem"},
			Prefix:               prefix,
		},
		client: client,
		logger: logger.NewLogger("test"),
	}
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		client := newFakeDataLakeClient()
		return storetest.Harness{
			Store:  newTestStore(client, prefix),
			Prefix: prefix,
			Names:  client.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*AzureDataLakeStorage)(nil)
}

func TestNewAzureDataLakeStorage(t *testing.T) {
	require.NotNil(t, NewAzureDataLakeStorage(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := NewAzureDataLakeStorage(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeDataLakeClient()
		require.NoError(t, newTestStore(client, "").Close())
		assert.True(t, client.closed)
	})
}

// --- provider specific behaviour ---

func TestCreateWithParent(t *testing.T) {
	notFound := &azcore.ResponseError{ErrorCode: string(datalakeerror.PathNotFound)}

	t.Run("no retry when the create succeeds", func(t *testing.T) {
		creates, dirs := 0, 0
		err := createWithParent(t.Context(), "a/b/file.bin",
			func(context.Context) error { creates++; return nil },
			func(context.Context, string) error { dirs++; return nil },
		)
		require.NoError(t, err)
		assert.Equal(t, 1, creates)
		assert.Zero(t, dirs, "the parent directory must not be created on the happy path")
	})

	t.Run("creates the parent directory once and retries", func(t *testing.T) {
		creates := 0
		var createdDir string
		err := createWithParent(t.Context(), "a/b/file.bin",
			func(context.Context) error {
				creates++
				if creates == 1 {
					return notFound
				}
				return nil
			},
			func(_ context.Context, dir string) error { createdDir = dir; return nil },
		)
		require.NoError(t, err)
		assert.Equal(t, 2, creates)
		assert.Equal(t, "a/b", createdDir, "the full parent path is created in one request")
	})

	t.Run("tolerates a concurrently created parent directory", func(t *testing.T) {
		creates := 0
		err := createWithParent(t.Context(), "a/b/file.bin",
			func(context.Context) error {
				creates++
				if creates == 1 {
					return notFound
				}
				return nil
			},
			func(context.Context, string) error {
				return &azcore.ResponseError{ErrorCode: string(datalakeerror.PathAlreadyExists)}
			},
		)
		require.NoError(t, err)
		assert.Equal(t, 2, creates)
	})

	t.Run("no parent to create", func(t *testing.T) {
		creates := 0
		err := createWithParent(t.Context(), "file.bin",
			func(context.Context) error { creates++; return notFound },
			func(context.Context, string) error { t.Fatal("must not create a directory"); return nil },
		)
		require.ErrorIs(t, err, notFound)
		assert.Equal(t, 1, creates)
	})

	t.Run("other errors are returned unchanged", func(t *testing.T) {
		sentinel := errors.New("boom")
		err := createWithParent(t.Context(), "a/b/file.bin",
			func(context.Context) error { return sentinel },
			func(context.Context, string) error { t.Fatal("must not create a directory"); return nil },
		)
		require.ErrorIs(t, err, sentinel)
	})
}

func TestSetOverwriteDoesNotMaskConditionErrors(t *testing.T) {
	// A ConditionNotMet raised while overwriting is not an existence conflict
	// and must not be reported as ErrFileAlreadyExists.
	client := newFakeDataLakeClient()
	client.putErr = &azcore.ResponseError{ErrorCode: string(datalakeerror.ConditionNotMet)}
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
		store := NewAzureDataLakeStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{})
		require.Error(t, err)
	})

	t.Run("missing file system name", func(t *testing.T) {
		store := NewAzureDataLakeStorage(logger.NewLogger("test"))
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
	md := newTestStore(newFakeDataLakeClient(), "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "FileSystemName")
	assert.Contains(t, md, "prefix")
}

// --- error classification ---

func TestErrorClassification(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		assert.True(t, isNotFound(&azcore.ResponseError{ErrorCode: string(datalakeerror.PathNotFound)}))
		assert.False(t, isNotFound(&azcore.ResponseError{ErrorCode: string(datalakeerror.ConditionNotMet)}))
		assert.False(t, isNotFound(errors.New("boom")))
	})

	t.Run("precondition failed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(datalakeerror.PathAlreadyExists)}))
		assert.True(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(datalakeerror.ConditionNotMet)}))
		assert.False(t, isPreconditionFailed(&azcore.ResponseError{ErrorCode: string(datalakeerror.PathNotFound)}))
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})
}

// --- fakes ---

type fakeDataLakeClient struct {
	objects map[string][]byte
	putErr  error
	closed  bool
}

func newFakeDataLakeClient() *fakeDataLakeClient {
	return &fakeDataLakeClient{objects: map[string][]byte{}}
}

func (f *fakeDataLakeClient) names() []string {
	names := make([]string, 0, len(f.objects))
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (f *fakeDataLakeClient) putObject(_ context.Context, name string, data io.Reader, overwrite bool) error {
	if f.putErr != nil {
		return f.putErr
	}
	if _, ok := f.objects[name]; ok && !overwrite {
		return &azcore.ResponseError{ErrorCode: string(datalakeerror.PathAlreadyExists)}
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.objects[name] = b
	return nil
}

func (f *fakeDataLakeClient) getObject(_ context.Context, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, &azcore.ResponseError{ErrorCode: string(datalakeerror.PathNotFound)}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeDataLakeClient) deleteObject(_ context.Context, name string) error {
	if _, ok := f.objects[name]; !ok {
		return &azcore.ResponseError{ErrorCode: string(datalakeerror.PathNotFound)}
	}
	delete(f.objects, name)
	return nil
}

func (f *fakeDataLakeClient) close() error {
	f.closed = true
	return nil
}
