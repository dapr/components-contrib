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

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	"github.com/dapr/kit/logger"
)

func newTestStore(client s3StoreClient, prefix string) *AWSS3 {
	return &AWSS3{
		metadata: &s3Metadata{Bucket: "test-bucket", Prefix: prefix},
		client:   client,
		logger:   logger.NewLogger("test"),
	}
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		client := newFakeS3Client()
		return storetest.Harness{
			Store:  newTestStore(client, prefix),
			Prefix: prefix,
			Names:  client.names,
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*AWSS3)(nil)
}

func TestNewAWSS3(t *testing.T) {
	require.NotNil(t, NewAWSS3(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("nil client", func(t *testing.T) {
		store := NewAWSS3(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured client", func(t *testing.T) {
		client := newFakeS3Client()
		require.NoError(t, newTestStore(client, "").Close())
		assert.True(t, client.closed)
	})
}

// --- provider specific behaviour ---

func TestDeleteWithoutReadPermission(t *testing.T) {
	// S3 answers HeadObject with 403 rather than 404 when the caller lacks
	// s3:ListBucket, so a delete-only policy must still be able to delete.
	client := newFakeS3Client()
	client.objects["file.bin"] = []byte("payload")
	client.headErr = &fakeAPIError{code: "AccessDenied"}
	store := newTestStore(client, "")

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))
	assert.NotContains(t, client.objects, "file.bin")
}

func TestDeleteSurfacesUnexpectedHeadErrors(t *testing.T) {
	client := newFakeS3Client()
	client.objects["file.bin"] = []byte("payload")
	client.headErr = errors.New("boom")
	store := newTestStore(client, "")

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"})
	require.Error(t, err)
	require.NotErrorIs(t, err, binarystore.ErrFileNotFound)
	assert.Contains(t, client.objects, "file.bin")
}

func TestSetOverwriteDoesNotMaskConflicts(t *testing.T) {
	// A conflict raised while overwriting (e.g. OperationAborted from racing
	// writers) is unrelated to object existence and must not be reported as
	// ErrFileAlreadyExists.
	client := newFakeS3Client()
	client.putErr = &fakeAPIError{code: "ConditionalRequestConflict"}
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

	t.Run("bucket and prefix", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{"bucket": "my-bucket", "PREFIX": "tenant-a"})
		require.NoError(t, err)
		assert.Equal(t, "my-bucket", m.Bucket)
		assert.Equal(t, "tenant-a", m.Prefix, "metadata keys must be matched case-insensitively")
	})

	t.Run("disableSSL adds an http scheme", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{
			"bucket":     "my-bucket",
			"endpoint":   "localhost:9000",
			"disableSSL": "true",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://localhost:9000", m.Endpoint)
	})

	t.Run("disableSSL preserves an existing scheme", func(t *testing.T) {
		m, err := parseMetadata(map[string]string{
			"bucket":     "my-bucket",
			"endpoint":   "https://localhost:9000",
			"disableSSL": "true",
		})
		require.NoError(t, err)
		assert.Equal(t, "https://localhost:9000", m.Endpoint)
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(newFakeS3Client(), "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "bucket")
	assert.Contains(t, md, "prefix")
}

// --- error classification ---

func TestErrorClassification(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		assert.True(t, isNotFound(&types.NoSuchKey{}))
		assert.True(t, isNotFound(&fakeAPIError{code: "NoSuchKey"}))
		assert.True(t, isNotFound(&fakeAPIError{code: "NotFound"}))
		assert.True(t, isNotFound(&fakeHTTPError{status: http.StatusNotFound}))
		assert.False(t, isNotFound(errors.New("boom")))
	})

	t.Run("precondition failed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&fakeAPIError{code: "PreconditionFailed"}))
		assert.True(t, isPreconditionFailed(&fakeAPIError{code: "ConditionalRequestConflict"}))
		assert.True(t, isPreconditionFailed(&fakeHTTPError{status: http.StatusPreconditionFailed}))
		// 409 alone is not an existence conflict on S3.
		assert.False(t, isPreconditionFailed(&fakeHTTPError{status: http.StatusConflict}))
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})

	t.Run("access denied", func(t *testing.T) {
		assert.True(t, isAccessDenied(&fakeAPIError{code: "AccessDenied"}))
		assert.True(t, isAccessDenied(&fakeHTTPError{status: http.StatusForbidden}))
		assert.False(t, isAccessDenied(errors.New("boom")))
	})
}

// --- fakes ---

type fakeS3Client struct {
	objects map[string][]byte
	putErr  error
	headErr error
	closed  bool
}

func newFakeS3Client() *fakeS3Client {
	return &fakeS3Client{objects: map[string][]byte{}}
}

func (f *fakeS3Client) names() []string {
	names := make([]string, 0, len(f.objects))
	for name := range f.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (f *fakeS3Client) putObject(_ context.Context, name string, data io.Reader, overwrite bool) error {
	if f.putErr != nil {
		return f.putErr
	}
	if _, ok := f.objects[name]; ok && !overwrite {
		return &fakeAPIError{code: "PreconditionFailed"}
	}
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.objects[name] = b
	return nil
}

func (f *fakeS3Client) getObject(_ context.Context, name string) (io.ReadCloser, error) {
	data, ok := f.objects[name]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeS3Client) headObject(_ context.Context, name string) error {
	if f.headErr != nil {
		return f.headErr
	}
	if _, ok := f.objects[name]; !ok {
		return &fakeAPIError{code: "NotFound"}
	}
	return nil
}

func (f *fakeS3Client) deleteObject(_ context.Context, name string) error {
	delete(f.objects, name)
	return nil
}

func (f *fakeS3Client) close() error {
	f.closed = true
	return nil
}

type fakeAPIError struct {
	code string
}

func (e *fakeAPIError) Error() string                 { return e.code }
func (e *fakeAPIError) ErrorCode() string             { return e.code }
func (e *fakeAPIError) ErrorMessage() string          { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

type fakeHTTPError struct {
	status int
}

func (e *fakeHTTPError) Error() string       { return http.StatusText(e.status) }
func (e *fakeHTTPError) HTTPStatusCode() int { return e.status }
