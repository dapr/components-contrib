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

package objectstorage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

func newTestStore(client objectStoreClient) *ObjectStorage {
	return &ObjectStorage{
		metadata: &objectStoreMetadata{BucketName: "test-bucket", Namespace: "test-namespace"},
		client:   client,
		logger:   logger.NewLogger("test"),
	}
}

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = NewOCIObjectStorage(logger.NewLogger("test"))
}

func TestNewOCIObjectStorage(t *testing.T) {
	store := NewOCIObjectStorage(logger.NewLogger("test"))
	require.NotNil(t, store)
}

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := &ObjectStorage{}
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeOCIClient()
		store := newTestStore(client)
		require.NoError(t, store.Close())
		assert.True(t, client.closed)
	})
}

func TestFeatures(t *testing.T) {
	store := newTestStore(newFakeOCIClient())
	features := store.Features()
	require.NotNil(t, features)
	assert.Empty(t, features)
}

func TestMissingFileName(t *testing.T) {
	store := newTestStore(newFakeOCIClient())

	err := store.Set(t.Context(), &binarystore.SetRequest{Data: strings.NewReader("payload")})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)

	err = store.Delete(t.Context(), &binarystore.DeleteRequest{})
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

func TestSetCreateOnlyAndOverwrite(t *testing.T) {
	client := newFakeOCIClient()
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
	client := newFakeOCIClient()
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
	client := newFakeOCIClient()
	client.objects["file.bin"] = []byte("payload")
	store := newTestStore(client)

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))
	assert.NotContains(t, client.objects, "file.bin")

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
}

func TestParseMetadata(t *testing.T) {
	t.Run("identity authentication", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{
			bucketNameKey:  "bucket",
			compartmentKey: "compartment",
			regionKey:      "region",
			userKey:        "user",
			fingerPrintKey: "fingerprint",
			privateKeyKey:  "private-key",
			tenancyKey:     "tenancy",
			"namespace":    "namespace",
		})
		require.NoError(t, err)
		assert.Equal(t, "bucket", m.BucketName)
		assert.Equal(t, "namespace", m.Namespace)
	})

	t.Run("missing bucket", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{compartmentKey: "compartment"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), bucketNameKey)
	})

	t.Run("missing compartment", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{bucketNameKey: "bucket"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), compartmentKey)
	})

	t.Run("instance principal skips identity fields", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{
			bucketNameKey:                      "bucket",
			compartmentKey:                     "compartment",
			instancePrincipalAuthenticationKey: "true",
		})
		require.NoError(t, err)
	})

	t.Run("config file path cannot use home shorthand", func(t *testing.T) {
		_, err := parseMetadata(map[string]string{
			bucketNameKey:               "bucket",
			compartmentKey:              "compartment",
			configFileAuthenticationKey: "true",
			configFilePathKey:           "~/config",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "~/")
	})
}

func TestGetComponentMetadata(t *testing.T) {
	store := newTestStore(newFakeOCIClient())
	md := store.GetComponentMetadata()
	require.NotNil(t, md)
	_, hasBucket := md["bucketName"]
	assert.True(t, hasBucket, "bucketName must appear in component metadata")
}

type fakeOCIClient struct {
	objects       map[string][]byte
	lastOverwrite bool
	lastReader    *trackingReadCloser
	closed        bool
}

func newFakeOCIClient() *fakeOCIClient {
	return &fakeOCIClient{objects: map[string][]byte{}}
}

func (f *fakeOCIClient) putObject(_ context.Context, name string, data io.Reader, overwrite bool) error {
	f.lastOverwrite = overwrite
	if _, ok := f.objects[name]; ok && !overwrite {
		return testServiceError{status: http.StatusPreconditionFailed, code: "PreconditionFailed"}
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.objects[name] = b
	return nil
}

func (f *fakeOCIClient) getObject(_ context.Context, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, testServiceError{status: http.StatusNotFound, code: "NotFound"}
	}
	f.lastReader = &trackingReadCloser{Reader: bytes.NewReader(data)}
	return f.lastReader, nil
}

func (f *fakeOCIClient) deleteObject(_ context.Context, name string) error {
	if _, ok := f.objects[name]; !ok {
		return testServiceError{status: http.StatusNotFound, code: "NotFound"}
	}
	delete(f.objects, name)
	return nil
}

func (f *fakeOCIClient) close() error {
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

type testServiceError struct {
	status int
	code   string
}

func (e testServiceError) Error() string          { return e.code }
func (e testServiceError) GetHTTPStatusCode() int { return e.status }
func (e testServiceError) GetCode() string        { return e.code }
