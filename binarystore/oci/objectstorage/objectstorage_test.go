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

package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	"github.com/dapr/kit/logger"
)

func newTestStore(client objectStoreClient, prefix string) *ObjectStorage {
	return &ObjectStorage{
		metadata: &objectStoreMetadata{
			BucketName: "test-bucket",
			Namespace:  "test-namespace",
			Prefix:     prefix,
		},
		client: client,
		logger: logger.NewLogger("test"),
	}
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		client := newFakeOCIClient()
		return storetest.Harness{
			Store:  newTestStore(client, prefix),
			Prefix: prefix,
			Names:  client.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*ObjectStorage)(nil)
}

func TestNewOCIObjectStorage(t *testing.T) {
	require.NotNil(t, NewOCIObjectStorage(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := NewOCIObjectStorage(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeOCIClient()
		require.NoError(t, newTestStore(client, "").Close())
		assert.True(t, client.closed)
	})
}

// --- provider specific behaviour ---

func TestSetPassesOverwriteToTheClient(t *testing.T) {
	client := newFakeOCIClient()
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
	client := newFakeOCIClient()
	client.putErr = testServiceError{status: http.StatusPreconditionFailed, code: "PreconditionFailed"}
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
			"PREFIX":       "tenant-a",
		})
		require.NoError(t, err)
		assert.Equal(t, "bucket", m.BucketName)
		assert.Equal(t, "namespace", m.Namespace)
		assert.Equal(t, "tenant-a", m.Prefix, "metadata keys must be matched case-insensitively")
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
	md := newTestStore(newFakeOCIClient(), "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, bucketNameKey)
	assert.Contains(t, md, "prefix")
}

// --- error classification ---

func TestErrorClassification(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		assert.True(t, isNotFound(testServiceError{status: http.StatusNotFound, code: "ObjectNotFound"}))
		assert.False(t, isNotFound(testServiceError{status: http.StatusConflict, code: "Conflict"}))
		assert.False(t, isNotFound(errors.New("boom")))
	})

	t.Run("precondition failed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(testServiceError{status: http.StatusPreconditionFailed, code: "PreconditionFailed"}))
		assert.True(t, isPreconditionFailed(testServiceError{status: http.StatusOK, code: "ConditionNotMet"}))
		assert.True(t, isPreconditionFailed(testServiceError{status: http.StatusConflict, code: "ObjectAlreadyExists"}))
		// A bare 409 is an unrelated conflict, not an existence conflict.
		assert.False(t, isPreconditionFailed(testServiceError{status: http.StatusConflict, code: "Conflict"}))
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})

	t.Run("already exists", func(t *testing.T) {
		assert.True(t, isAlreadyExists(testServiceError{status: http.StatusConflict, code: "BucketAlreadyExists"}))
		assert.False(t, isAlreadyExists(testServiceError{status: http.StatusConflict, code: "Conflict"}))
		assert.False(t, isAlreadyExists(testServiceError{status: http.StatusNotFound, code: "BucketAlreadyExists"}))
	})
}

// --- fakes ---

type fakeOCIClient struct {
	objects       map[string][]byte
	lastOverwrite bool
	putErr        error
	closed        bool
}

func newFakeOCIClient() *fakeOCIClient {
	return &fakeOCIClient{objects: map[string][]byte{}}
}

func (f *fakeOCIClient) names() []string {
	names := make([]string, 0, len(f.objects))
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (f *fakeOCIClient) putObject(_ context.Context, name string, data io.Reader, overwrite bool) error {
	f.lastOverwrite = overwrite
	if f.putErr != nil {
		return f.putErr
	}
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
		return nil, testServiceError{status: http.StatusNotFound, code: "ObjectNotFound"}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeOCIClient) deleteObject(_ context.Context, name string) error {
	if _, ok := f.objects[name]; !ok {
		return testServiceError{status: http.StatusNotFound, code: "ObjectNotFound"}
	}
	delete(f.objects, name)
	return nil
}

func (f *fakeOCIClient) close() error {
	f.closed = true
	return nil
}

type testServiceError struct {
	status int
	code   string
}

func (e testServiceError) Error() string          { return e.code }
func (e testServiceError) GetHTTPStatusCode() int { return e.status }
func (e testServiceError) GetCode() string        { return e.code }
