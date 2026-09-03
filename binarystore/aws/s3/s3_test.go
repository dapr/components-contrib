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
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/kit/logger"
)

// newTestStore returns an uninitialised AWSS3 cast to its concrete type so
// that unit tests can call unexported helpers without a live AWS connection.
func newTestStore() *AWSS3 {
	return NewAWSS3(logger.NewLogger("test")).(*AWSS3)
}

// --- interface compliance ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = NewAWSS3(logger.NewLogger("test"))
}

// --- constructor ---

func TestNewAWSS3(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAWSS3(log)
	require.NotNil(t, store)
}

// --- Close ---

func TestClose(t *testing.T) {
	store := newTestStore()
	require.NoError(t, store.Close())
}

// --- Features ---

func TestFeatures(t *testing.T) {
	store := newTestStore()
	features := store.Features()
	require.NotNil(t, features)
	assert.Empty(t, features)
}

// --- Set validation (no AWS connection required) ---

func TestSet_MissingFileName(t *testing.T) {
	store := newTestStore()

	err := store.Set(t.Context(), &binarystore.SetRequest{
		Data:      strings.NewReader("payload"),
		Overwrite: true,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- Get validation (no AWS connection required) ---

func TestGet_MissingFileName(t *testing.T) {
	store := newTestStore()

	_, err := store.Get(t.Context(), &binarystore.GetRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- Delete validation (no AWS connection required) ---

func TestDelete_MissingFileName(t *testing.T) {
	store := newTestStore()

	err := store.Delete(t.Context(), &binarystore.DeleteRequest{})

	require.Error(t, err)
	require.ErrorIs(t, err, binarystore.ErrMissingFileName)
}

// --- SetRequest semantics ---

func TestSetRequest_OverwriteDefaultsFalse(t *testing.T) {
	req := &binarystore.SetRequest{
		FileName: "test.bin",
		Data:     strings.NewReader("hello"),
	}
	assert.False(t, req.Overwrite, "zero value of Overwrite must be false (create-only semantics)")
}

func TestSetRequest_OverwriteCanBeSetTrue(t *testing.T) {
	req := &binarystore.SetRequest{
		FileName:  "test.bin",
		Data:      strings.NewReader("hello"),
		Overwrite: true,
	}
	assert.True(t, req.Overwrite)
}

// --- Sentinel error identity ---

func TestSentinelErrors(t *testing.T) {
	t.Run("ErrFileAlreadyExists wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrFileAlreadyExists, binarystore.ErrFileAlreadyExists)
	})

	t.Run("ErrFileNotFound wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrFileNotFound, binarystore.ErrFileNotFound)
	})

	t.Run("ErrMissingFileName wraps correctly", func(t *testing.T) {
		require.ErrorIs(t, binarystore.ErrMissingFileName, binarystore.ErrMissingFileName)
	})

	t.Run("sentinel errors are distinct", func(t *testing.T) {
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrFileNotFound)
		assert.NotEqual(t, binarystore.ErrFileAlreadyExists, binarystore.ErrMissingFileName)
		assert.NotEqual(t, binarystore.ErrFileNotFound, binarystore.ErrMissingFileName)
	})
}

// --- GetComponentMetadata ---

func TestGetComponentMetadata(t *testing.T) {
	store := newTestStore()
	md := store.GetComponentMetadata()
	require.NotNil(t, md)
	_, hasBucket := md["bucket"]
	assert.True(t, hasBucket, "bucket must appear in component metadata")
}

// --- metadata parsing ---

func TestParseMetadata_MissingBucket(t *testing.T) {
	_, err := parseMetadata(map[string]string{})
	require.Error(t, err)
}

func TestParseMetadata_Bucket(t *testing.T) {
	m, err := parseMetadata(map[string]string{"bucket": "my-bucket"})
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", m.Bucket)
}

func TestParseMetadata_DisableSSLAddsHTTPPrefix(t *testing.T) {
	m, err := parseMetadata(map[string]string{
		"bucket":     "my-bucket",
		"endpoint":   "localhost:9000",
		"disableSSL": "true",
	})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:9000", m.Endpoint)
}

func TestParseMetadata_DisableSSLPreservesExistingScheme(t *testing.T) {
	m, err := parseMetadata(map[string]string{
		"bucket":     "my-bucket",
		"endpoint":   "https://localhost:9000",
		"disableSSL": "true",
	})
	require.NoError(t, err)
	assert.Equal(t, "https://localhost:9000", m.Endpoint)
}

// --- error mapping helpers ---

type fakeAPIError struct {
	code string
}

func (e *fakeAPIError) Error() string        { return e.code }
func (e *fakeAPIError) ErrorCode() string    { return e.code }
func (e *fakeAPIError) ErrorMessage() string { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}

func TestIsNotFound(t *testing.T) {
	t.Run("NoSuchKey type", func(t *testing.T) {
		assert.True(t, isNotFound(&types.NoSuchKey{}))
	})

	t.Run("API error code NoSuchKey", func(t *testing.T) {
		assert.True(t, isNotFound(&fakeAPIError{code: "NoSuchKey"}))
	})

	t.Run("API error code NotFound", func(t *testing.T) {
		assert.True(t, isNotFound(&fakeAPIError{code: "NotFound"}))
	})

	t.Run("unrelated error", func(t *testing.T) {
		assert.False(t, isNotFound(errors.New("boom")))
	})
}

func TestIsPreconditionFailed(t *testing.T) {
	t.Run("API error code PreconditionFailed", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&fakeAPIError{code: "PreconditionFailed"}))
	})

	t.Run("API error code ConditionalRequestConflict", func(t *testing.T) {
		assert.True(t, isPreconditionFailed(&fakeAPIError{code: "ConditionalRequestConflict"}))
	})

	t.Run("unrelated error", func(t *testing.T) {
		assert.False(t, isPreconditionFailed(errors.New("boom")))
	})
}
