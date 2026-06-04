/*
Copyright 2021 The Dapr Authors
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
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

func TestGetOption(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error if blobName is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.get(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobName)
	})
}

func TestDeleteOption(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error if blobName is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.delete(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobName)
	})

	t.Run("return error for invalid deleteSnapshots", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		r.Metadata = map[string]string{
			"blobName":        "foo",
			"deleteSnapshots": "invalid",
		}
		_, err := blobStorage.delete(t.Context(), &r)
		require.Error(t, err)
	})
}

func TestGetConcurrency(t *testing.T) {
	t.Run("returns default when nil", func(t *testing.T) {
		c, err := getConcurrency(nil)
		require.NoError(t, err)
		assert.Equal(t, int(defaultConcurrency), c)
	})

	t.Run("returns parsed value when valid", func(t *testing.T) {
		c, err := getConcurrency(ptr.Of(int32(20)))
		require.NoError(t, err)
		assert.Equal(t, 20, c)
	})

	t.Run("returns error when zero", func(t *testing.T) {
		_, err := getConcurrency(ptr.Of(int32(0)))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})

	t.Run("returns error when negative", func(t *testing.T) {
		_, err := getConcurrency(ptr.Of(int32(-5)))
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})

	t.Run("returns 1 when set to 1", func(t *testing.T) {
		c, err := getConcurrency(ptr.Of(int32(1)))
		require.NoError(t, err)
		assert.Equal(t, 1, c)
	})
}

func TestBulkGetValidation(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error when neither items nor prefix is provided", func(t *testing.T) {
		payload := bulkGetPayload{}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobSelection)
	})

	t.Run("return error when payload is nil", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobSelection)
	})

	t.Run("return error when payload is invalid JSON", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data: []byte(`not json`),
		}
		_, err := blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error parsing bulkGet payload")
	})

	t.Run("return error when items have only empty blobNames", func(t *testing.T) {
		tmpDir := t.TempDir()
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "", FilePath: ptr.Of(filepath.Join(tmpDir, "a"))},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobSelection)
	})

	t.Run("accept item with filePath for file mode", func(t *testing.T) {
		tmpDir := t.TempDir()
		expectedPath := filepath.Join(tmpDir, "blob1")
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "blob1", FilePath: ptr.Of(expectedPath)},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		// Verify the payload is valid (no validation errors about filePath).
		var parsed bulkGetPayload
		require.NoError(t, json.Unmarshal(data, &parsed))
		require.Len(t, parsed.Items, 1)
		require.NotNil(t, parsed.Items[0].FilePath)
		assert.Equal(t, expectedPath, *parsed.Items[0].FilePath)
	})

	t.Run("accept item without filePath for inline mode", func(t *testing.T) {
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "blob1"},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		// Verify the payload is valid — nil filePath means inline mode.
		var parsed bulkGetPayload
		require.NoError(t, json.Unmarshal(data, &parsed))
		require.Len(t, parsed.Items, 1)
		assert.Nil(t, parsed.Items[0].FilePath)
	})

	t.Run("return error when prefix is set without destinationDir", func(t *testing.T) {
		payload := bulkGetPayload{
			Prefix: ptr.Of("some-prefix/"),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingDestinationDir)
	})

	t.Run("return error when concurrency is zero", func(t *testing.T) {
		tmpDir := t.TempDir()
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "blob1", FilePath: ptr.Of(filepath.Join(tmpDir, "blob1"))},
			},
			Concurrency: ptr.Of(int32(0)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})

	t.Run("return error when concurrency is negative", func(t *testing.T) {
		tmpDir := t.TempDir()
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "blob1", FilePath: ptr.Of(filepath.Join(tmpDir, "blob1"))},
			},
			Concurrency: ptr.Of(int32(-1)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkGet(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})
}

func TestBulkCreateValidation(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error when items array is empty", func(t *testing.T) {
		payload := bulkCreatePayload{}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEmptyBulkCreateItems)
	})

	t.Run("return error when payload is nil", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrEmptyBulkCreateItems)
	})

	t.Run("return error when payload is invalid JSON", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data: []byte(`not json`),
		}
		_, err := blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error parsing bulkCreate payload")
	})

	t.Run("return error when item has empty blobName", func(t *testing.T) {
		tmpDir := t.TempDir()
		payload := bulkCreatePayload{
			Items: []bulkCreateItem{
				{BlobName: "valid", SourcePath: filepath.Join(tmpDir, "file")},
				{BlobName: "", SourcePath: filepath.Join(tmpDir, "file")},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "item at index 1 is missing blobName")
	})

	t.Run("return error when item has neither data nor sourcePath", func(t *testing.T) {
		payload := bulkCreatePayload{
			Items: []bulkCreateItem{
				{BlobName: "blob1"},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "item at index 0 is missing both data and sourcePath")
	})

	t.Run("accept item with inline data", func(t *testing.T) {
		payload := bulkCreatePayload{
			Items: []bulkCreateItem{
				{BlobName: "blob1", Data: ptr.Of("hello")},
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		// Verify the payload is valid — data is an acceptable source.
		var parsed bulkCreatePayload
		require.NoError(t, json.Unmarshal(data, &parsed))
		require.Len(t, parsed.Items, 1)
		require.NotNil(t, parsed.Items[0].Data)
		assert.Equal(t, "hello", *parsed.Items[0].Data)
	})

	t.Run("return error when concurrency is zero", func(t *testing.T) {
		tmpDir := t.TempDir()
		payload := bulkCreatePayload{
			Items: []bulkCreateItem{
				{BlobName: "blob1", SourcePath: filepath.Join(tmpDir, "file")},
			},
			Concurrency: ptr.Of(int32(0)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkCreate(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})
}

func TestBulkDeleteValidation(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error when both prefix and blobNames are empty", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data: []byte(`{}`),
		}
		_, err := blobStorage.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBulkDeleteSelection)
	})

	t.Run("return error when payload is nil", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBulkDeleteSelection)
	})

	t.Run("return error when payload is invalid JSON", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Data: []byte(`not json`),
		}
		_, err := blobStorage.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "error parsing bulkDelete payload")
	})

	t.Run("return error when deleteSnapshots is invalid", func(t *testing.T) {
		payload := bulkDeletePayload{
			BlobNames:       []string{"blob1"},
			DeleteSnapshots: ptr.Of("invalidOption"),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid delete snapshot option type")
	})

	t.Run("return error when concurrency is negative", func(t *testing.T) {
		payload := bulkDeletePayload{
			BlobNames:   []string{"blob1"},
			Concurrency: ptr.Of(int32(-1)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		r := bindings.InvokeRequest{Data: data}
		_, err = blobStorage.bulkDelete(t.Context(), &r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidConcurrency)
	})
}

func TestOperationsIncludesBulk(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)
	ops := blobStorage.Operations()

	expectedOps := []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
		bulkGetOperation,
		bulkCreateOperation,
		bulkDeleteOperation,
	}

	for _, expected := range expectedOps {
		found := false
		for _, op := range ops {
			if op == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "expected operation %q not found in Operations()", expected)
	}
}

func TestInvokeUnsupportedOperation(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	r := bindings.InvokeRequest{
		Operation: "unsupported",
	}
	_, err := blobStorage.Invoke(t.Context(), &r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported operation")
}

func TestBulkPayloadParsing(t *testing.T) {
	t.Run("bulkGet payload with concurrency and filePath pointer", func(t *testing.T) {
		tmpDir := t.TempDir()
		expectedPath := filepath.Join(tmpDir, "b.txt")
		payload := bulkGetPayload{
			Items: []bulkGetItem{
				{BlobName: "a.txt"},
				{BlobName: "b.txt", FilePath: ptr.Of(expectedPath)},
			},
			Concurrency: ptr.Of(int32(5)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		var p bulkGetPayload
		err = json.Unmarshal(data, &p)
		require.NoError(t, err)
		require.Len(t, p.Items, 2)
		assert.Equal(t, "a.txt", p.Items[0].BlobName)
		assert.Nil(t, p.Items[0].FilePath)
		assert.Equal(t, "b.txt", p.Items[1].BlobName)
		require.NotNil(t, p.Items[1].FilePath)
		assert.Equal(t, expectedPath, *p.Items[1].FilePath)
		require.NotNil(t, p.Concurrency)
		assert.Equal(t, int32(5), *p.Concurrency)
	})

	t.Run("bulkGet payload without concurrency is nil", func(t *testing.T) {
		raw := `{"items":[{"blobName":"a.txt"}]}`
		var p bulkGetPayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Nil(t, p.Concurrency)
	})

	t.Run("bulkCreate payload with inline data and contentType", func(t *testing.T) {
		tmpDir := t.TempDir()
		expectedPath := filepath.Join(tmpDir, "f2.txt")
		payload := bulkCreatePayload{
			Items: []bulkCreateItem{
				{BlobName: "f1.txt", Data: ptr.Of("hello")},
				{BlobName: "f2.txt", SourcePath: expectedPath, ContentType: ptr.Of("text/plain")},
			},
			Concurrency: ptr.Of(int32(3)),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		var p bulkCreatePayload
		err = json.Unmarshal(data, &p)
		require.NoError(t, err)
		assert.Len(t, p.Items, 2)
		assert.Equal(t, "f1.txt", p.Items[0].BlobName)
		require.NotNil(t, p.Items[0].Data)
		assert.Equal(t, "hello", *p.Items[0].Data)
		assert.Empty(t, p.Items[0].SourcePath)
		assert.Nil(t, p.Items[0].ContentType)
		assert.Equal(t, "f2.txt", p.Items[1].BlobName)
		assert.Equal(t, expectedPath, p.Items[1].SourcePath)
		assert.Nil(t, p.Items[1].Data)
		require.NotNil(t, p.Items[1].ContentType)
		assert.Equal(t, "text/plain", *p.Items[1].ContentType)
		require.NotNil(t, p.Concurrency)
		assert.Equal(t, int32(3), *p.Concurrency)
	})

	t.Run("bulkCreate item with neither data nor sourcePath", func(t *testing.T) {
		raw := `{"items":[{"blobName":"f1.txt"}]}`
		var p bulkCreatePayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Nil(t, p.Items[0].Data)
		assert.Empty(t, p.Items[0].SourcePath)
	})

	t.Run("bulkDelete payload", func(t *testing.T) {
		raw := `{"blobNames":["x.txt","y.txt"]}`
		var p bulkDeletePayload
		err := json.Unmarshal([]byte(raw), &p)
		require.NoError(t, err)
		assert.Equal(t, []string{"x.txt", "y.txt"}, p.BlobNames)
	})
}

func TestBulkGetResponseItemJSON(t *testing.T) {
	t.Run("omits empty fields", func(t *testing.T) {
		r := bulkGetResponseItem{BlobName: "test.txt"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "filePath")
		assert.NotContains(t, string(data), "data")
		assert.NotContains(t, string(data), "error")
		assert.Contains(t, string(data), `"blobName":"test.txt"`)
	})

	t.Run("includes data when present", func(t *testing.T) {
		r := bulkGetResponseItem{BlobName: "test.txt", Data: []byte("hello")}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		// []byte is marshalled as base64 by encoding/json.
		assert.Contains(t, string(data), `"data":"aGVsbG8="`)
	})

	t.Run("includes filePath when present", func(t *testing.T) {
		tmpDir := t.TempDir()
		fp := filepath.Join(tmpDir, "test.txt")
		r := bulkGetResponseItem{BlobName: "test.txt", FilePath: fp}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"filePath":`)
		var decoded bulkGetResponseItem
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.Equal(t, fp, decoded.FilePath)
	})

	t.Run("includes error when present", func(t *testing.T) {
		r := bulkGetResponseItem{BlobName: "test.txt", Error: "not found"}
		data, err := json.Marshal(r)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"error":"not found"`)
	})
}

func TestMaxBatchDeleteSize(t *testing.T) {
	assert.Equal(t, 256, maxBatchDeleteSize)
}

func TestPresignOption(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error if blobName is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Metadata: map[string]string{
				"signTTL": "15m",
			},
		}
		_, err := blobStorage.presign(&r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingBlobName)
	})

	t.Run("return error if signTTL is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Metadata: map[string]string{
				"blobName": "test-blob",
			},
		}
		_, err := blobStorage.presign(&r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingSignTTL)
	})

	t.Run("return error if signTTL is empty", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Metadata: map[string]string{
				"blobName": "test-blob",
				"signTTL":  "",
			},
		}
		_, err := blobStorage.presign(&r)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrMissingSignTTL)
	})

	t.Run("return error if signTTL is invalid duration", func(t *testing.T) {
		// Need a container client for this test path (past metadata validation)
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		r := bindings.InvokeRequest{
			Metadata: map[string]string{
				"blobName": "test-blob",
				"signTTL":  "not-a-duration",
			},
		}
		_, err = blobStorage.presign(&r)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot parse signTTL duration")
	})

	t.Run("generate SAS URL with valid shared key credential", func(t *testing.T) {
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		r := bindings.InvokeRequest{
			Metadata: map[string]string{
				"blobName": "test-blob",
				"signTTL":  "15m",
			},
		}
		resp, err := blobStorage.presign(&r)
		require.NoError(t, err)
		require.NotNil(t, resp)

		var presignResp presignResponse
		err = json.Unmarshal(resp.Data, &presignResp)
		require.NoError(t, err)
		assert.Contains(t, presignResp.PresignURL, "testaccount.blob.core.windows.net")
		assert.Contains(t, presignResp.PresignURL, "testcontainer")
		assert.Contains(t, presignResp.PresignURL, "test-blob")
		assert.Contains(t, presignResp.PresignURL, "sig=")
		assert.Contains(t, presignResp.PresignURL, "se=")
		assert.Contains(t, presignResp.PresignURL, "sp=r")
	})
}

func TestPresignViaInvoke(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
	require.NoError(t, err)
	client, err := container.NewClientWithSharedKeyCredential(
		"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
	)
	require.NoError(t, err)
	blobStorage.containerClient = client

	t.Run("invoke presign operation", func(t *testing.T) {
		r := bindings.InvokeRequest{
			Operation: presignOperation,
			Metadata: map[string]string{
				"blobName": "test-blob",
				"signTTL":  "1h",
			},
		}
		resp, err := blobStorage.Invoke(t.Context(), &r)
		require.NoError(t, err)
		require.NotNil(t, resp)

		var presignResp presignResponse
		err = json.Unmarshal(resp.Data, &presignResp)
		require.NoError(t, err)
		assert.NotEmpty(t, presignResp.PresignURL)
		assert.Contains(t, presignResp.PresignURL, "sig=")
	})
}

func TestGenerateSASURL(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)

	t.Run("return error for invalid TTL", func(t *testing.T) {
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		blockBlobClient := client.NewBlockBlobClient("test-blob")
		_, err = blobStorage.generateSASURL(blockBlobClient, "invalid")
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot parse signTTL duration")
	})

	t.Run("return error for zero TTL", func(t *testing.T) {
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		blockBlobClient := client.NewBlockBlobClient("test-blob")
		_, err = blobStorage.generateSASURL(blockBlobClient, "0s")
		require.Error(t, err)
		require.Contains(t, err.Error(), "signTTL must be a positive duration")
	})

	t.Run("return error for negative TTL", func(t *testing.T) {
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		blockBlobClient := client.NewBlockBlobClient("test-blob")
		_, err = blobStorage.generateSASURL(blockBlobClient, "-5m")
		require.Error(t, err)
		require.Contains(t, err.Error(), "signTTL must be a positive duration")
	})

	t.Run("generate valid SAS URL", func(t *testing.T) {
		cred, err := azblob.NewSharedKeyCredential("testaccount", "dGVzdGtleQ==")
		require.NoError(t, err)
		client, err := container.NewClientWithSharedKeyCredential(
			"https://testaccount.blob.core.windows.net/testcontainer", cred, nil,
		)
		require.NoError(t, err)
		blobStorage.containerClient = client

		blockBlobClient := client.NewBlockBlobClient("myfile.txt")
		sasURL, err := blobStorage.generateSASURL(blockBlobClient, "30m")
		require.NoError(t, err)
		assert.Contains(t, sasURL, "testaccount.blob.core.windows.net")
		assert.Contains(t, sasURL, "testcontainer")
		assert.Contains(t, sasURL, "myfile.txt")
		assert.Contains(t, sasURL, "sig=")
		assert.Contains(t, sasURL, "sp=r")
	})
}

func TestOperationsIncludesPresign(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)
	ops := blobStorage.Operations()

	found := false
	for _, op := range ops {
		if op == presignOperation {
			found = true
			break
		}
	}
	assert.True(t, found, "Operations() should include presign")
}
