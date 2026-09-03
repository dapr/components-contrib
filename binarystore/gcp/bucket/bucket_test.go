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

package bucket

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

func newTestStore(client gcsClient) *GCPBucket {
	return &GCPBucket{
		metadata: &gcpMetadata{Bucket: "test-bucket"},
		client:   client,
		logger:   logger.NewLogger("test"),
	}
}

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = NewGCPBucket(logger.NewLogger("test"))
}

func TestNewGCPBucket(t *testing.T) {
	store := NewGCPBucket(logger.NewLogger("test"))
	require.NotNil(t, store)
}

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := &GCPBucket{}
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeGCSClient()
		store := newTestStore(client)
		require.NoError(t, store.Close())
		assert.True(t, client.closed)
	})
}

func TestFeatures(t *testing.T) {
	store := newTestStore(newFakeGCSClient())
	features := store.Features()
	require.NotNil(t, features)
	assert.Empty(t, features)
}

func TestMissingFileName(t *testing.T) {
	store := newTestStore(newFakeGCSClient())

	err := store.Set(t.Context(), &binarystore.SetRequest{Data: strings.NewReader("payload")})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	err = store.Delete(t.Context(), &binarystore.DeleteRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

func TestSetCreateOnlyAndOverwrite(t *testing.T) {
	client := newFakeGCSClient()
	store := newTestStore(client)

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("first"),
	})
	require.NoError(t, err)
	assert.False(t, client.lastOverwrite)

	err = store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("second"),
	})
	require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
	assert.Equal(t, []byte("first"), client.objects["file.bin"])

	err = store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      strings.NewReader("second"),
		Overwrite: true,
	})
	require.NoError(t, err)
	assert.True(t, client.lastOverwrite)
	assert.Equal(t, []byte("second"), client.objects["file.bin"])
}

func TestGetStreamingBodyAndNotFound(t *testing.T) {
	client := newFakeGCSClient()
	client.objects["file.bin"] = []byte("payload")
	store := newTestStore(client)

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)

	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(data))
	assert.False(t, client.lastReader.closed)
	require.NoError(t, resp.Data.Close())
	assert.True(t, client.lastReader.closed)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "missing.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestDeleteAndNotFound(t *testing.T) {
	client := newFakeGCSClient()
	client.objects["file.bin"] = []byte("payload")
	store := newTestStore(client)

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))
	assert.NotContains(t, client.objects, "file.bin")

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestParseMetadata(t *testing.T) {
	t.Run("missing bucket", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{})
		require.Error(t, err)
	})

	t.Run("bucket and gcp auth fields", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{
			"bucket":        "my-bucket",
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
		assert.Equal(t, "project", m.ProjectID)
		assert.Equal(t, "key-id", m.PrivateKeyID)
		assert.Equal(t, "client@example.com", m.ClientEmail)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	store := newTestStore(newFakeGCSClient())
	md := store.GetComponentMetadata()
	require.NotNil(t, md)
	_, hasBucket := md["bucket"]
	assert.True(t, hasBucket, "bucket must appear in component metadata")
}

type fakeGCSClient struct {
	objects       map[string][]byte
	lastOverwrite bool
	lastReader    *trackingReadCloser
	closed        bool
}

func newFakeGCSClient() *fakeGCSClient {
	return &fakeGCSClient{objects: map[string][]byte{}}
}

func (f *fakeGCSClient) putObject(_ context.Context, _, name string, data io.Reader, overwrite bool) error {
	f.lastOverwrite = overwrite
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
	f.lastReader = &trackingReadCloser{Reader: bytes.NewReader(data)}
	return f.lastReader, nil
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

type trackingReadCloser struct {
	*bytes.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}
