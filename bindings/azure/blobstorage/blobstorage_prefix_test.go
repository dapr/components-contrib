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
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	storagecommon "github.com/dapr/components-contrib/common/component/azure/blobstorage"
	"github.com/dapr/kit/logger"
)

// This file contains regression tests for the review finding that blobName
// unconditionally prepends the configured component prefix to every
// caller-supplied name, while list (and friends) returned Azure's raw blob
// names -- which already contain the prefix. A name that came out of list
// and was passed back into get/delete/presign/bulkGet/bulkDelete therefore
// got the prefix applied twice, silently failing (delete treats
// BlobNotFound as success). The tests below spin up a fake Azure Blob REST
// server so the fix (stripping the prefix from names handed back to
// callers) can be exercised end to end, not just at the unit level.

// requestRecorder captures every request the fake server receives so tests
// can assert what was actually sent over the wire (i.e. the fully-prefixed
// name), independent of what the binding returns to the caller.
type requestRecorder struct {
	mu       sync.Mutex
	requests []string // "METHOD /path"
}

func (r *requestRecorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req.Method+" "+req.URL.Path)
}

func (r *requestRecorder) hasExact(method, path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	target := method + " " + path
	for _, req := range r.requests {
		if req == target {
			return true
		}
	}
	return false
}

const listBlobsXMLTemplate = `<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults ServiceEndpoint="%s" ContainerName="testcontainer">
  <Prefix>%s</Prefix>
  <Marker></Marker>
  <MaxResults>5000</MaxResults>
  <Blobs>%s</Blobs>
  <NextMarker></NextMarker>
</EnumerationResults>`

func blobItemXML(name string) string {
	return fmt.Sprintf(`<Blob><Name>%s</Name><Properties>`+
		`<Content-Length>3</Content-Length><Etag>0x1</Etag>`+
		`<Last-Modified>Wed, 09 Sep 2009 09:20:02 GMT</Last-Modified>`+
		`<BlobType>BlockBlob</BlobType></Properties></Blob>`, name)
}

// newFakeBlobServer starts an httptest server that emulates just enough of
// the Azure Blob REST API (list, download, single delete and batch delete)
// for the prefix-handling paths in list/get/delete/bulkGet/bulkDelete to be
// exercised without a live storage account. Batch delete is always made to
// fail so bulkDelete falls back to its (much simpler to fake) individual
// per-blob delete path.
func newFakeBlobServer(t *testing.T, recorder *requestRecorder, blobNames []string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		recorder.record(req)
		q := req.URL.Query()

		switch {
		case req.Method == http.MethodGet && q.Get("comp") == "list":
			var items strings.Builder
			for _, name := range blobNames {
				items.WriteString(blobItemXML(name))
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, listBlobsXMLTemplate, "http://"+req.Host, q.Get("prefix"), items.String())

		case req.Method == http.MethodPost && q.Get("comp") == "batch":
			// Force the batch delete API to fail so bulkDelete exercises its
			// individual-delete fallback path instead.
			w.WriteHeader(http.StatusBadRequest)

		case req.Method == http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)

		case req.Method == http.MethodHead:
			// Blob properties, issued by the SDK before a download-to-file.
			w.Header().Set("Content-Length", "3")
			w.Header().Set("ETag", `"0x1"`)
			w.Header().Set("Last-Modified", "Wed, 09 Sep 2009 09:20:02 GMT")
			w.Header().Set("x-ms-blob-type", "BlockBlob")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)

		case req.Method == http.MethodGet:
			// Plain blob download.
			body := "abc"
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
			w.Header().Set("ETag", `"0x1"`)
			w.Header().Set("Last-Modified", "Wed, 09 Sep 2009 09:20:02 GMT")
			w.Header().Set("x-ms-blob-type", "BlockBlob")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))

		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestBlobStorageWithPrefix(t *testing.T, server *httptest.Server, prefix string) *AzureBlobStorage {
	t.Helper()
	client, err := container.NewClientWithNoCredential(server.URL+"/testcontainer", nil)
	require.NoError(t, err)

	blobStorage := NewAzureBlobStorage(logger.NewLogger("test")).(*AzureBlobStorage)
	blobStorage.containerClient = client
	blobStorage.metadata = &storagecommon.BlobStorageMetadata{Prefix: prefix}
	return blobStorage
}

func TestListStripsConfiguredPrefixFromReturnedNames(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, []string{"tenantA/report.pdf", "tenantA/nested/file.txt"})
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	resp, err := blobStorage.list(t.Context(), &bindings.InvokeRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)

	var items []*container.BlobItem
	require.NoError(t, json.Unmarshal(resp.Data, &items))
	require.Len(t, items, 2)

	names := make([]string, 0, len(items))
	for _, item := range items {
		require.NotNil(t, item.Name)
		names = append(names, *item.Name)
	}
	// Names must be prefix-free: list must not hand back Azure's raw names
	// (which already contain the configured prefix).
	assert.ElementsMatch(t, []string{"report.pdf", "nested/file.txt"}, names)
}

func TestGetAfterListDoesNotDoublePrefix(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, []string{"tenantA/report.pdf"})
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	resp, err := blobStorage.list(t.Context(), &bindings.InvokeRequest{})
	require.NoError(t, err)

	var items []*container.BlobItem
	require.NoError(t, json.Unmarshal(resp.Data, &items))
	require.Len(t, items, 1)
	require.NotNil(t, items[0].Name)
	listedName := *items[0].Name
	assert.Equal(t, "report.pdf", listedName)

	// Feed the name returned by list straight back into get, exactly as a
	// caller would.
	getReq := &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataKeyBlobName: listedName,
		},
	}
	_, err = blobStorage.get(t.Context(), getReq)
	require.NoError(t, err)

	assert.True(t, recorder.hasExact(http.MethodGet, "/testcontainer/tenantA/report.pdf"),
		"expected the get request to use the single-prefixed blob path")
	assert.False(t, recorder.hasExact(http.MethodGet, "/testcontainer/tenantA/tenantA/report.pdf"),
		"get must not double-prefix a name that came back from list")
}

func TestDeleteAfterListDoesNotDoublePrefix(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, []string{"tenantA/report.pdf"})
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	resp, err := blobStorage.list(t.Context(), &bindings.InvokeRequest{})
	require.NoError(t, err)

	var items []*container.BlobItem
	require.NoError(t, json.Unmarshal(resp.Data, &items))
	require.Len(t, items, 1)
	listedName := *items[0].Name

	delReq := &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataKeyBlobName: listedName,
		},
	}
	_, err = blobStorage.delete(t.Context(), delReq)
	require.NoError(t, err)

	assert.True(t, recorder.hasExact(http.MethodDelete, "/testcontainer/tenantA/report.pdf"),
		"expected the delete request to target the single-prefixed blob path")
	assert.False(t, recorder.hasExact(http.MethodDelete, "/testcontainer/tenantA/tenantA/report.pdf"),
		"delete must not double-prefix a name that came back from list, "+
			"since a BlobNotFound there would be silently treated as success")
}

func TestBulkGetResponseStripsConfiguredPrefix(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, nil)
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	req := &bindings.InvokeRequest{
		Data: []byte(`{"items":[{"blobName":"report.pdf"}]}`),
	}
	resp, err := blobStorage.bulkGet(t.Context(), req)
	require.NoError(t, err)

	var results []bulkGetResponseItem
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Error)
	// The response handed back to the caller must be prefix-free...
	assert.Equal(t, "report.pdf", results[0].BlobName)
	// ...even though the actual request on the wire used the fully
	// prefixed name.
	assert.True(t, recorder.hasExact(http.MethodGet, "/testcontainer/tenantA/report.pdf"))
}

func TestBulkGetPrefixDiscoveryDoesNotLeakPrefixIntoDestination(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, []string{"tenantA/bulkprefix/a.txt", "tenantA/bulkprefix/nested/b.txt"})
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	destDir := t.TempDir()
	req := &bindings.InvokeRequest{
		Data: []byte(fmt.Sprintf(`{"prefix":"bulkprefix/","destinationDir":%q}`, filepath.ToSlash(destDir))),
	}
	resp, err := blobStorage.bulkGet(t.Context(), req)
	require.NoError(t, err)

	var results []bulkGetResponseItem
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 2)
	for _, result := range results {
		assert.Empty(t, result.Error)
	}

	// The on-disk layout mirrors the caller-visible (prefix-free) names, so
	// the component prefix is not reproduced under the destination dir.
	assert.FileExists(t, filepath.Join(destDir, "bulkprefix", "a.txt"))
	assert.FileExists(t, filepath.Join(destDir, "bulkprefix", "nested", "b.txt"))
	assert.NoDirExists(t, filepath.Join(destDir, "tenantA"))

	// The blobs themselves are still fetched using their fully prefixed names.
	assert.True(t, recorder.hasExact(http.MethodGet, "/testcontainer/tenantA/bulkprefix/a.txt"))
	assert.True(t, recorder.hasExact(http.MethodGet, "/testcontainer/tenantA/bulkprefix/nested/b.txt"))
}

func TestBulkDeleteResponseStripsConfiguredPrefix(t *testing.T) {
	recorder := &requestRecorder{}
	server := newFakeBlobServer(t, recorder, nil)
	blobStorage := newTestBlobStorageWithPrefix(t, server, "tenantA")

	req := &bindings.InvokeRequest{
		Data: []byte(`{"blobNames":["report.pdf"]}`),
	}
	resp, err := blobStorage.bulkDelete(t.Context(), req)
	require.NoError(t, err)

	var results []bulkDeleteResponseItem
	require.NoError(t, json.Unmarshal(resp.Data, &results))
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Error)
	// The response handed back to the caller must be prefix-free...
	assert.Equal(t, "report.pdf", results[0].BlobName)
	// ...even though the actual delete on the wire used the fully prefixed
	// name (confirming the blob that was actually targeted).
	assert.True(t, recorder.hasExact(http.MethodDelete, "/testcontainer/tenantA/report.pdf"))
}
