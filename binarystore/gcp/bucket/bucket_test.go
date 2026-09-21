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

package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	"github.com/dapr/kit/logger"
)

func newTestStore(client gcsClient, prefix string) *GCPBucket {
	return &GCPBucket{
		metadata: &gcpMetadata{Bucket: "test-bucket", Prefix: prefix},
		client:   client,
		logger:   logger.NewLogger("test"),
	}
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		client := newFakeGCSClient()
		return storetest.Harness{
			Store:  newTestStore(client, prefix),
			Prefix: prefix,
			Names:  client.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*GCPBucket)(nil)
}

func TestNewGCPBucket(t *testing.T) {
	require.NotNil(t, NewGCPBucket(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := NewGCPBucket(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeGCSClient()
		require.NoError(t, newTestStore(client, "").Close())
		assert.True(t, client.closed)
	})
}

// --- provider specific behaviour ---

func TestSetPassesOverwriteToTheClient(t *testing.T) {
	client := newFakeGCSClient()
	store := newTestStore(client, "")

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     bytes.NewReader([]byte("payload")),
	}))
	assert.False(t, client.lastOverwrite)

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      bytes.NewReader([]byte("payload")),
		Overwrite: true,
	}))
	assert.True(t, client.lastOverwrite)
}

func TestSetOverwriteDoesNotMaskPreconditionErrors(t *testing.T) {
	// A 412 raised while overwriting is not an existence conflict and must not
	// be reported as ErrFileAlreadyExists.
	client := newFakeGCSClient()
	client.putErr = &googleapi.Error{Code: http.StatusPreconditionFailed}
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
	t.Run("missing bucket", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{})
		require.Error(t, err)
	})

	t.Run("bucket and gcp auth fields", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{
			"bucket":        "my-bucket",
			"PREFIX":        "tenant-a",
			"project_id":    "project",
			"privateKeyID":  "key-id",
			"client_email":  "client@example.com",
			"private_key":   "private",
			"tokenURI":      "token",
			"client_id":     "client",
			"type":          "service_account",
			"auth_uri":      "auth",
			"clientCertURL": "cert",
		})
		require.NoError(t, err)
		assert.Equal(t, "my-bucket", m.Bucket)
		assert.Equal(t, "tenant-a", m.Prefix, "metadata keys must be matched case-insensitively")
		assert.Equal(t, "project", m.ProjectID)
		assert.Equal(t, "key-id", m.PrivateKeyID)
		assert.Equal(t, "client@example.com", m.ClientEmail)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(newFakeGCSClient(), "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "bucket")
	assert.Contains(t, md, "prefix")
}

// --- error classification ---

func TestErrorClassification(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		assert.True(t, isNotFound(storage.ErrObjectNotExist))
		assert.True(t, isNotFound(&googleapi.Error{Code: http.StatusNotFound}))
		assert.False(t, isNotFound(errors.New("boom")))
	})

	t.Run("precondition failed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&googleapi.Error{Code: http.StatusPreconditionFailed}))
		// 409 alone is not an existence conflict on Cloud Storage.
		assert.False(t, isPreconditionFailed(&googleapi.Error{Code: http.StatusConflict}))
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})
}

// --- fakes ---

type fakeGCSClient struct {
	objects       map[string][]byte
	lastOverwrite bool
	putErr        error
	closed        bool
}

func newFakeGCSClient() *fakeGCSClient {
	return &fakeGCSClient{objects: map[string][]byte{}}
}

func (f *fakeGCSClient) names() []string {
	names := make([]string, 0, len(f.objects))
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (f *fakeGCSClient) putObject(_ context.Context, _, name string, data io.Reader, overwrite bool) error {
	f.lastOverwrite = overwrite
	if f.putErr != nil {
		return f.putErr
	}
	if _, ok := f.objects[name]; ok && !overwrite {
		return &googleapi.Error{Code: http.StatusPreconditionFailed, Message: "conditionNotMet"}
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.objects[name] = b
	return nil
}

func (f *fakeGCSClient) getObject(_ context.Context, _, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, &googleapi.Error{Code: http.StatusNotFound, Message: "not found"}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeGCSClient) deleteObject(_ context.Context, _, name string) error {
	if _, ok := f.objects[name]; !ok {
		return &googleapi.Error{Code: http.StatusNotFound, Message: "not found"}
	}
	delete(f.objects, name)
	return nil
}

func (f *fakeGCSClient) close() error {
	f.closed = true
	return nil
}
