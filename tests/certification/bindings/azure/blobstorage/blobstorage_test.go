/*
Copyright 2022 The Dapr Authors
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

package azureblobstoragebinding_test

import (
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings/azure/blobstorage"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const (
	sidecarName  = "blobstorage-sidecar"
	blobNotFound = "blob not found"
)

// getBlobRequest is used to make a common binding request for the get operation.
func getBlobRequest(ctx flow.Context, client daprsdk.Client, name string, includeMetadata bool) (out *daprsdk.BindingEvent, err error) {
	fetchMetdata := fmt.Sprint(includeMetadata)
	invokeGetMetadata := map[string]string{
		"blobName":        name,
		"includeMetadata": fetchMetdata,
	}

	invokeGetRequest := &daprsdk.InvokeBindingRequest{
		Name:      "azure-blobstorage-output",
		Operation: "get",
		Data:      nil,
		Metadata:  invokeGetMetadata,
	}

	out, invokeGetErr := client.InvokeBinding(ctx, invokeGetRequest)
	if invokeGetErr != nil {
		return nil, fmt.Errorf("%w", invokeGetErr)
	}
	return out, nil
}

// listBlobRequest is used to make a common binding request for the list operation.
func listBlobRequest(ctx flow.Context, client daprsdk.Client, prefix string, marker string, maxResults int, includeMetadata bool, includeSnapshots bool, includeUncommittedBlobs bool, includeCopy bool, includeDeleted bool) (out *daprsdk.BindingEvent, err error) {
	requestOptions := make(map[string]interface{})

	requestOptions["prefix"] = prefix
	requestOptions["marker"] = marker
	if maxResults > -1 {
		requestOptions["maxResults"] = maxResults
	}
	includeOptions := make(map[string]interface{})
	includeOptions["snapshots"] = includeSnapshots
	includeOptions["uncommittedBlobs"] = includeUncommittedBlobs
	includeOptions["copy"] = includeCopy
	includeOptions["deleted"] = includeDeleted
	includeOptions["metadata"] = includeMetadata
	requestOptions["include"] = includeOptions

	optionsBytes, marshalErr := json.Marshal(requestOptions)
	if marshalErr != nil {
		return nil, fmt.Errorf("%w", marshalErr)
	}

	invokeRequest := &daprsdk.InvokeBindingRequest{
		Name:      "azure-blobstorage-output",
		Operation: "list",
		Data:      optionsBytes,
		Metadata:  nil,
	}

	out, invokeErr := client.InvokeBinding(ctx, invokeRequest)
	if invokeErr != nil {
		return nil, fmt.Errorf("%w", invokeErr)
	}
	return out, nil
}

// deleteBlobRequest is used to make a common binding request for the delete operation.
func deleteBlobRequest(ctx flow.Context, client daprsdk.Client, name string, deleteSnapshotsOption *string) (out *daprsdk.BindingEvent, err error) {
	invokeDeleteMetadata := map[string]string{
		"blobName": name,
	}
	if deleteSnapshotsOption != nil {
		invokeDeleteMetadata["deleteSnapshots"] = *deleteSnapshotsOption
	}

	invokeGetRequest := &daprsdk.InvokeBindingRequest{
		Name:      "azure-blobstorage-output",
		Operation: "delete",
		Data:      nil,
		Metadata:  invokeDeleteMetadata,
	}

	out, invokeDeleteErr := client.InvokeBinding(ctx, invokeGetRequest)
	if invokeDeleteErr != nil {
		return nil, fmt.Errorf("%w", invokeDeleteErr)
	}
	return out, nil
}

func TestBlobStorage(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]

	testCreateBlobWithFileNameConflict := func(ctx flow.Context) error {
		// verifies that overwriting a blob with the same name will not cause a conflict.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)

		invokeCreateMetadata := map[string]string{
			"contentType": "text/plain",
		}

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes,
			Metadata:  invokeCreateMetadata,
		}

		out, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)
		require.NoError(t, invokeCreateErr)

		blobName := out.Metadata["blobName"]
		res, _ := getBlobRequest(ctx, client, blobName, false)
		oldString := string(res.Data)

		input2 := "some other example content"
		dataBytes2 := []byte(input2)

		invokeCreateMetadata2 := map[string]string{
			"blobName":    blobName,
			"contentType": "text/plain",
		}

		invokeCreateRequest2 := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes2,
			Metadata:  invokeCreateMetadata2,
		}
		_, invokeCreateErr2 := client.InvokeBinding(ctx, invokeCreateRequest2)

		require.NoError(t, invokeCreateErr2)

		res2, _ := getBlobRequest(ctx, client, blobName, false)
		newString := string(res2.Data)

		assert.NotEqual(t, oldString, newString)
		assert.Equal(t, newString, input2)

		// cleanup.
		out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
		require.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), blobNotFound)

		// deleting the key again should fail.
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, blobName, nil)
		assert.Error(t, invokeDeleteErr2)
		assert.Contains(t, invokeDeleteErr2.Error(), blobNotFound)

		return nil
	}

	testCreateBlobInvalidContentHash := func(ctx flow.Context) error {
		// verifies that the content hash is validated.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)
		wrongBytesForContentHash := []byte("wrong content to hash")
		h := md5.New() //nolint:gosec
		h.Write(wrongBytesForContentHash)
		md5HashBase64 := base64.StdEncoding.EncodeToString(h.Sum(nil))

		invokeCreateMetadata := map[string]string{
			"contentMD5": md5HashBase64,
		}

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes,
			Metadata:  invokeCreateMetadata,
		}

		_, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)
		assert.Error(t, invokeCreateErr)
		assert.Contains(t, invokeCreateErr.Error(), bloberror.MD5Mismatch)

		return nil
	}

	testCreateBlobFromFile := func(isBase64 bool) func(ctx flow.Context) error {
		// uploads a blob from a file and optionally verifies the automatic base64 decoding option.
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			dataBytes := []byte("somecontent")
			if isBase64 {
				dataBytes = []byte(base64.StdEncoding.EncodeToString(dataBytes))
			}

			invokeCreateRequest := &daprsdk.InvokeBindingRequest{
				Name:      "azure-blobstorage-output",
				Operation: "create",
				Data:      dataBytes,
				Metadata:  nil,
			}

			out, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)
			require.NoError(t, invokeCreateErr)

			blobName := out.Metadata["blobName"]

			out, invokeGetErr := getBlobRequest(ctx, client, blobName, false)
			require.NoError(t, invokeGetErr)
			responseData := out.Data
			if isBase64 {
				// input was automatically base64 decoded.
				// for comparison we will base64 encode the response data.
				responseData = []byte(base64.StdEncoding.EncodeToString(out.Data))
			}
			assert.Equal(t, responseData, dataBytes)
			assert.Empty(t, out.Metadata)

			out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
			require.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion.
			_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
			assert.Error(t, invokeSecondGetErr)
			assert.Contains(t, invokeSecondGetErr.Error(), blobNotFound)

			return nil
		}
	}

	testCreatePublicBlob := func(shoudBePublic bool, containerName string) func(ctx flow.Context) error {
		// creates a blob and verifies whether it is public or not,
		//   this depends on how the binding and the backing blob container are configured.
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			inputBytes := []byte("this is a public blob")
			invokeCreateRequest := &daprsdk.InvokeBindingRequest{
				Name:      "azure-blobstorage-output",
				Operation: "create",
				Data:      inputBytes,
				Metadata:  map[string]string{},
			}

			out, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)
			require.NoError(t, invokeCreateErr)

			blobName := out.Metadata["blobName"]
			storageAccountName := os.Getenv("AzureBlobStorageAccount")
			if containerName == "" {
				containerName = os.Getenv("AzureBlobStorageContainer")
			}

			// verify the blob is public via http request.
			url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", storageAccountName, containerName, blobName)
			resp, httpErr := http.Get(url) //nolint:gosec
			require.NoError(t, httpErr)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if shoudBePublic {
				assert.Less(t, resp.StatusCode, 400)
				assert.Equal(t, inputBytes, body)
			} else {
				assert.Greater(t, resp.StatusCode, 399)
			}

			// cleanup.
			_, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
			require.NoError(t, invokeDeleteErr)

			return nil
		}
	}

	testCreateGetListDelete := func(ctx flow.Context) error {
		// basic test of create, get, list, delete operations.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)
		h := md5.New() //nolint:gosec
		h.Write(dataBytes)
		md5HashBase64 := base64.StdEncoding.EncodeToString(h.Sum(nil))

		invokeCreateMetadata := map[string]string{
			"blobName":           "filename.txt",
			"contentType":        "text/plain",
			"contentMD5":         md5HashBase64,
			"contentEncoding":    "UTF-8",
			"contentLanguage":    "en-us",
			"contentDisposition": "attachment",
			"cacheControl":       "no-cache",
			"custom":             "hello-world",
		}

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      dataBytes,
			Metadata:  invokeCreateMetadata,
		}

		_, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)

		require.NoError(t, invokeCreateErr)

		invokeGetMetadata := map[string]string{
			"blobName":        "filename.txt",
			"includeMetadata": "true",
		}

		invokeGetRequest := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "get",
			Data:      nil,
			Metadata:  invokeGetMetadata,
		}

		out, invokeGetErr := client.InvokeBinding(ctx, invokeGetRequest)
		require.NoError(t, invokeGetErr)
		assert.Equal(t, input, string(out.Data))
		assert.Contains(t, out.Metadata, "Custom")
		assert.Equal(t, "hello-world", out.Metadata["Custom"])

		out, invokeErr := listBlobRequest(ctx, client, "", "", -1, true, false, false, false, false)
		require.NoError(t, invokeErr)
		var output []map[string]interface{}
		unmarshalErr := json.Unmarshal(out.Data, &output)
		require.NoError(t, unmarshalErr)

		found := false
		for _, item := range output {
			if item["Name"] == "filename.txt" {
				found = true
				properties, ok := item["Properties"].(map[string]interface{})
				assert.True(t, ok)
				assert.Equal(t, properties["ContentMD5"], invokeCreateMetadata["contentMD5"])
				assert.Equal(t, properties["ContentType"], invokeCreateMetadata["contentType"])
				assert.Equal(t, properties["CacheControl"], invokeCreateMetadata["cacheControl"])
				assert.Equal(t, properties["ContentDisposition"], invokeCreateMetadata["contentDisposition"])
				assert.Equal(t, properties["ContentEncoding"], invokeCreateMetadata["contentEncoding"])
				assert.Equal(t, properties["ContentLanguage"], invokeCreateMetadata["contentLanguage"])
				assert.Equal(t, item["Metadata"].(map[string]interface{})["custom"], invokeCreateMetadata["custom"])
				break
			}
		}
		assert.True(t, found)

		out, invokeDeleteErr := deleteBlobRequest(ctx, client, "filename.txt", nil)
		require.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getBlobRequest(ctx, client, "filename.txt", false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), blobNotFound)

		return nil
	}

	testListContents := func(ctx flow.Context) error {
		// simply runs the list contents operation.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		_, invokeErr := listBlobRequest(ctx, client, "", "", -1, false, false, false, false, false)
		require.NoError(t, invokeErr)

		return invokeErr
	}

	testListContentsWithOptions := func(ctx flow.Context) error {
		// verifies the list contents operation with several options:
		// - prefix specified to limit the list to a specific filename prefix
		// - marker specified to start the list from a specific blob name
		// - maxResults specified to limit the number of results returned
		// - includeMetadata specified to include the custom metadata in the list
		// - includeDeleted specified to include the deleted blobs in the list.

		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// create a blob with a prefix of "prefixA".
		invokeCreateMetadata1 := map[string]string{
			"blobName": "prefixA/filename.txt",
		}

		invokeCreateRequest1 := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      []byte("some example content"),
			Metadata:  invokeCreateMetadata1,
		}

		_, invokeCreateErr1 := client.InvokeBinding(ctx, invokeCreateRequest1)
		require.NoError(t, invokeCreateErr1)

		// create another blob with a prefix of "prefixA".
		invokeCreateMetadata2 := map[string]string{
			"blobName": "prefixAfilename.txt",
		}

		invokeCreateRequest2 := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      []byte("some example content"),
			Metadata:  invokeCreateMetadata2,
		}

		_, invokeCreateErr2 := client.InvokeBinding(ctx, invokeCreateRequest2)
		require.NoError(t, invokeCreateErr2)

		// create a blob with a prefix of "prefixB".
		invokeCreateMetadata3 := map[string]string{
			"blobName": "prefixB/filename.txt",
		}

		invokeCreateRequest3 := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      []byte("some example content"),
			Metadata:  invokeCreateMetadata3,
		}

		_, invokeCreateErr3 := client.InvokeBinding(ctx, invokeCreateRequest3)
		require.NoError(t, invokeCreateErr3)

		// list the contents of the container.
		out, listErr := listBlobRequest(ctx, client, "prefixA", "", 1, false, false, false, false, false)
		require.NoError(t, listErr)

		var output []map[string]interface{}
		unmarshalErr := json.Unmarshal(out.Data, &output)
		require.NoError(t, unmarshalErr)

		assert.Equal(t, 1, len(output))
		assert.Contains(t, output[0]["Name"], "prefixA")

		nextMarker := out.Metadata["marker"]
		assert.Empty(t, nextMarker)

		assert.Equal(t, "1", out.Metadata["pagesTraversed"])
		assert.Equal(t, "1", out.Metadata["number"])

		// Commenting this out for now. We do not have enough data to for a second page of results, so cannot test this.

		// // list the contents of the container with a marker.
		// out2, listErr2 := listBlobRequest(ctx, client, "prefix", nextMarker, 1, false, false, false, false, false)
		// require.NoError(t, listErr2)

		// var output2 []map[string]interface{}
		// err2 := json.Unmarshal(out2.Data, &output2)
		// require.NoError(t, err2)

		// assert.Equal(t, 1, len(output2))
		// assert.Contains(t, output2[0]["Name"], "prefixA")

		// nextMarker2 := out2.Metadata["marker"]
		// assert.Empty(t, nextMarker2)

		// assert.Equal(t, "1", out2.Metadata["pagesTraversed"])

		// cleanup.
		_, invokeDeleteErr1 := deleteBlobRequest(ctx, client, "prefixA/filename.txt", nil)
		require.NoError(t, invokeDeleteErr1)
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, "prefixAfilename.txt", nil)
		require.NoError(t, invokeDeleteErr2)
		_, invokeDeleteErr3 := deleteBlobRequest(ctx, client, "prefixB/filename.txt", nil)
		require.NoError(t, invokeDeleteErr3)

		// list deleted items with prefix.
		out3, listErr3 := listBlobRequest(ctx, client, "prefixA/", "", -1, false, false, false, false, true)
		require.NoError(t, listErr3)

		// this will only return the deleted items if soft delete policy is enabled for the blob service.
		assert.Equal(t, "1", out3.Metadata["number"])
		var output3 []map[string]interface{}
		err3 := json.Unmarshal(out3.Data, &output3)
		require.NoError(t, err3)
		assert.Equal(t, len(output3), 1)

		return nil
	}

	testSnapshotDeleteAndList := func(ctx flow.Context) error {
		// verifies the list operation can list snapshots.
		// verifies the delete operation can delete snapshots.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		cred, _ := azblob.NewSharedKeyCredential(os.Getenv("AzureBlobStorageAccount"), os.Getenv("AzureBlobStorageAccessKey"))
		containerClient, _ := container.NewClientWithSharedKeyCredential(fmt.Sprintf("https://%s.blob.core.windows.net/%s", os.Getenv("AzureBlobStorageAccount"), os.Getenv("AzureBlobStorageContainer")), cred, nil)

		blobClient := containerClient.NewBlockBlobClient("snapshotthis.txt")
		_, uploadErr := blobClient.UploadBuffer(
			ctx, []byte("some example content"),
			&azblob.UploadBufferOptions{}) //nolint:exhaustivestruct
		require.NoError(t, uploadErr)
		_, createSnapshotErr := blobClient.CreateSnapshot(
			ctx, &blob.CreateSnapshotOptions{}) //nolint:exhaustivestruct
		require.NoError(t, createSnapshotErr)

		// list the contents of the container including snapshots for the specific blob only.
		out, listErr := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		require.NoError(t, listErr)
		assert.Equal(t, out.Metadata["number"], "2")

		// delete snapshots.
		_, invokeDeleteErr := deleteBlobRequest(ctx, client, "snapshotthis.txt", ptr.Of(string(blob.DeleteSnapshotsOptionTypeOnly)))
		require.NoError(t, invokeDeleteErr)

		// verify snapshot is deleted.
		out2, listErr2 := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		require.NoError(t, listErr2)
		assert.Equal(t, "1", out2.Metadata["number"])

		// create another snapshot.
		_, createSnapshotErr2 := blobClient.CreateSnapshot(
			ctx, &blob.CreateSnapshotOptions{}) //nolint:exhaustivestruct
		require.NoError(t, createSnapshotErr2)

		// delete base blob and snapshots all at once.
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, "snapshotthis.txt", ptr.Of(string(blob.DeleteSnapshotsOptionTypeInclude)))
		require.NoError(t, invokeDeleteErr2)

		// verify base blob and snapshots are deleted.
		out3, listErr3 := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		require.NoError(t, listErr3)
		assert.Equal(t, "0", out3.Metadata["number"])

		return nil
	}

	testBulkCreateGetDelete := func(ctx flow.Context) error {
		// Tests the full lifecycle of bulk operations:
		// bulkCreate → bulkGet → bulkDelete.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// Create temp source files.
		sourceDir := t.TempDir()
		blobNames := []string{"bulk/file1.txt", "bulk/file2.txt", "bulk/file3.txt"}
		fileContents := []string{"content of file 1", "content of file 2", "content of file 3"}

		items := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			filePath := filepath.Join(sourceDir, name)
			require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
			require.NoError(t, os.WriteFile(filePath, []byte(fileContents[i]), 0o644))
			items[i] = map[string]string{
				"blobName":   name,
				"sourcePath": filePath,
			}
		}

		// bulkCreate — upload all files.
		createPayload := map[string]interface{}{
			"items": items,
		}
		createData, marshalErr := json.Marshal(createPayload)
		require.NoError(t, marshalErr)

		_, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkCreate",
			Data:      createData,
		})
		require.NoError(t, invokeCreateErr)

		// bulkGet — download all files to individual target paths.
		destDir := t.TempDir()
		getItems := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			getItems[i] = map[string]string{
				"blobName": name,
				"filePath": filepath.Join(destDir, name),
			}
		}
		getPayload := map[string]interface{}{
			"items": getItems,
		}
		getData, marshalErr := json.Marshal(getPayload)
		require.NoError(t, marshalErr)

		getOut, invokeGetErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkGet",
			Data:      getData,
		})
		require.NoError(t, invokeGetErr)

		// Verify response contains all items.
		var getResults []map[string]interface{}
		require.NoError(t, json.Unmarshal(getOut.Data, &getResults))
		assert.Len(t, getResults, len(blobNames))

		// Verify downloaded file contents.
		for i, name := range blobNames {
			downloaded, readErr := os.ReadFile(filepath.Join(destDir, name))
			require.NoError(t, readErr)
			assert.Equal(t, fileContents[i], string(downloaded))
		}

		// bulkDelete — delete all blobs by explicit names.
		deletePayload := map[string]interface{}{
			"blobNames": blobNames,
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)

		_, invokeDeleteErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, invokeDeleteErr)

		// Verify all blobs are deleted.
		listOut, listErr := listBlobRequest(ctx, client, "bulk/", "", -1, false, false, false, false, false)
		require.NoError(t, listErr)
		assert.Equal(t, "0", listOut.Metadata["number"])

		return nil
	}

	testBulkGetByPrefix := func(ctx flow.Context) error {
		// Tests bulkGet using prefix-based selection.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// Create some blobs with a common prefix.
		blobNames := []string{"bulkprefix/a.txt", "bulkprefix/b.txt"}
		for _, name := range blobNames {
			_, err := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
				Name:      "azure-blobstorage-output",
				Operation: "create",
				Data:      []byte("content of " + name),
				Metadata:  map[string]string{"blobName": name},
			})
			require.NoError(t, err)
		}

		// bulkGet by prefix.
		destDir := t.TempDir()
		getPayload := map[string]interface{}{
			"prefix":         "bulkprefix/",
			"destinationDir": destDir,
		}
		getData, marshalErr := json.Marshal(getPayload)
		require.NoError(t, marshalErr)

		getOut, invokeGetErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkGet",
			Data:      getData,
		})
		require.NoError(t, invokeGetErr)

		var getResults []map[string]interface{}
		require.NoError(t, json.Unmarshal(getOut.Data, &getResults))
		assert.Len(t, getResults, 2)

		// Verify files exist on disk.
		for _, name := range blobNames {
			data, readErr := os.ReadFile(filepath.Join(destDir, name))
			require.NoError(t, readErr)
			assert.Equal(t, "content of "+name, string(data))
		}

		// Cleanup via bulkDelete with prefix.
		deletePayload := map[string]interface{}{
			"prefix": "bulkprefix/",
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)
		_, invokeDeleteErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, invokeDeleteErr)

		return nil
	}

	testGetToFile := func(ctx flow.Context) error {
		// Tests the single get operation with filePath metadata to stream to file.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// Create a blob.
		content := "streaming get to file test content"
		blobName := "gettofile-test.txt"
		_, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      []byte(content),
			Metadata:  map[string]string{"blobName": blobName},
		})
		require.NoError(t, invokeCreateErr)

		// Get with filePath — should stream to file.
		destDir := t.TempDir()
		filePath := filepath.Join(destDir, blobName)
		out, invokeGetErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "get",
			Metadata: map[string]string{
				"blobName": blobName,
				"filePath": filePath,
			},
		})
		require.NoError(t, invokeGetErr)

		// Response data should be empty (streamed to file).
		assert.Empty(t, out.Data)
		assert.Equal(t, filePath, out.Metadata["filePath"])

		// Verify file content.
		downloaded, readErr := os.ReadFile(filePath)
		require.NoError(t, readErr)
		assert.Equal(t, content, string(downloaded))

		// Cleanup.
		_, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
		require.NoError(t, invokeDeleteErr)

		return nil
	}

	testBulkCreateWithBase64 := func(ctx flow.Context) error {
		// Tests bulkCreate with decodeBase64 enabled — source files contain
		// base64-encoded data, which should be decoded during streaming upload.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		sourceDir := t.TempDir()
		originalContent := []string{"binary content one", "binary content two"}
		blobNames := []string{"b64bulk/file1.bin", "b64bulk/file2.bin"}

		items := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			// Write base64-encoded content to source files.
			encoded := base64.StdEncoding.EncodeToString([]byte(originalContent[i]))
			filePath := filepath.Join(sourceDir, name)
			require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
			require.NoError(t, os.WriteFile(filePath, []byte(encoded), 0o644))
			items[i] = map[string]string{
				"blobName":   name,
				"sourcePath": filePath,
			}
		}

		// bulkCreate with decodeBase64 component config.
		createPayload := map[string]interface{}{
			"items": items,
		}
		createData, marshalErr := json.Marshal(createPayload)
		require.NoError(t, marshalErr)

		_, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkCreate",
			Data:      createData,
		})
		require.NoError(t, invokeCreateErr)

		// Verify uploaded blobs contain decoded (original) content.
		for i, name := range blobNames {
			out, getErr := getBlobRequest(ctx, client, name, false)
			require.NoError(t, getErr)
			assert.Equal(t, originalContent[i], string(out.Data))
		}

		// Cleanup via bulkDelete.
		deletePayload := map[string]interface{}{
			"blobNames": blobNames,
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)
		_, invokeDeleteErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, invokeDeleteErr)

		return nil
	}

	testBulkCreatePlainEncoding := func(ctx flow.Context) error {
		// Tests bulkCreate without base64 — source files contain plain
		// text that should be uploaded as-is.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		sourceDir := t.TempDir()
		contents := []string{"plain text file one", "plain text file two", "plain text file three"}
		blobNames := []string{"plainbulk/a.txt", "plainbulk/b.txt", "plainbulk/c.txt"}

		items := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			filePath := filepath.Join(sourceDir, name)
			require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
			require.NoError(t, os.WriteFile(filePath, []byte(contents[i]), 0o644))
			items[i] = map[string]string{
				"blobName":   name,
				"sourcePath": filePath,
			}
		}

		createPayload := map[string]interface{}{
			"items": items,
		}
		createData, marshalErr := json.Marshal(createPayload)
		require.NoError(t, marshalErr)

		_, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkCreate",
			Data:      createData,
		})
		require.NoError(t, invokeCreateErr)

		// Verify blobs contain exact plain text.
		for i, name := range blobNames {
			out, getErr := getBlobRequest(ctx, client, name, false)
			require.NoError(t, getErr)
			assert.Equal(t, contents[i], string(out.Data))
		}

		// bulkGet to individual file paths.
		destDir := t.TempDir()
		getItems := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			getItems[i] = map[string]string{
				"blobName": name,
				"filePath": filepath.Join(destDir, name),
			}
		}
		getPayload := map[string]interface{}{
			"items": getItems,
		}
		getData, marshalErr := json.Marshal(getPayload)
		require.NoError(t, marshalErr)

		_, invokeGetErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkGet",
			Data:      getData,
		})
		require.NoError(t, invokeGetErr)

		// Verify downloaded files.
		for i, name := range blobNames {
			downloaded, readErr := os.ReadFile(filepath.Join(destDir, name))
			require.NoError(t, readErr)
			assert.Equal(t, contents[i], string(downloaded))
		}

		// Cleanup.
		deletePayload := map[string]interface{}{
			"blobNames": blobNames,
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)
		_, cleanupErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, cleanupErr, "cleanup bulkDelete failed")

		return nil
	}

	testBulkCreateInlineData := func(ctx flow.Context) error {
		// Tests bulkCreate using inline data (no source files).
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		blobNames := []string{"inline/a.txt", "inline/b.txt"}
		contents := []string{"inline content A", "inline content B"}

		// bulkCreate with inline data.
		items := make([]map[string]interface{}, len(blobNames))
		for i, name := range blobNames {
			items[i] = map[string]interface{}{
				"blobName": name,
				"data":     contents[i],
			}
		}
		createPayload := map[string]interface{}{
			"items":       items,
			"concurrency": 2,
		}
		createData, marshalErr := json.Marshal(createPayload)
		require.NoError(t, marshalErr)

		createOut, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkCreate",
			Data:      createData,
		})
		require.NoError(t, invokeCreateErr)

		var createResults []map[string]interface{}
		require.NoError(t, json.Unmarshal(createOut.Data, &createResults))
		require.Len(t, createResults, 2)
		for _, r := range createResults {
			assert.Empty(t, r["error"], "expected no error for %v", r["blobName"])
			assert.NotEmpty(t, r["blobURL"], "expected blobURL for %v", r["blobName"])
		}

		// Verify via regular get.
		for i, name := range blobNames {
			out, getErr := getBlobRequest(ctx, client, name, false)
			require.NoError(t, getErr)
			assert.Equal(t, contents[i], string(out.Data))
		}

		// Cleanup.
		deletePayload := map[string]interface{}{
			"blobNames": blobNames,
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)
		_, cleanupErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, cleanupErr, "cleanup bulkDelete failed")

		return nil
	}

	testBulkGetInlineData := func(ctx flow.Context) error {
		// Tests bulkGet returning data inline (no filePath).
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// Create blobs first.
		blobNames := []string{"inlineget/x.txt", "inlineget/y.txt"}
		contents := []string{"data for x", "data for y"}
		for i, name := range blobNames {
			_, err := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
				Name:      "azure-blobstorage-output",
				Operation: "create",
				Data:      []byte(contents[i]),
				Metadata:  map[string]string{"blobName": name},
			})
			require.NoError(t, err)
		}

		// bulkGet without filePath — should return data inline.
		getItems := make([]map[string]string, len(blobNames))
		for i, name := range blobNames {
			getItems[i] = map[string]string{
				"blobName": name,
			}
		}
		getPayload := map[string]interface{}{
			"items": getItems,
		}
		getData, marshalErr := json.Marshal(getPayload)
		require.NoError(t, marshalErr)

		getOut, invokeGetErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkGet",
			Data:      getData,
		})
		require.NoError(t, invokeGetErr)

		var getResults []map[string]interface{}
		require.NoError(t, json.Unmarshal(getOut.Data, &getResults))
		require.Len(t, getResults, 2)

		expected := map[string]string{}
		for i, name := range blobNames {
			expected[name] = contents[i]
		}
		for _, r := range getResults {
			blobName := r["blobName"].(string)
			assert.Empty(t, r["error"], "expected no error for %s", blobName)
			// Data is []byte in the response struct, which encoding/json marshals as base64.
			// When unmarshalled into map[string]interface{}, it arrives as a base64 string.
			dataB64, ok := r["data"].(string)
			require.True(t, ok, "data should be a base64 string for %s", blobName)
			decoded, decErr := base64.StdEncoding.DecodeString(dataB64)
			require.NoError(t, decErr, "failed to decode base64 data for %s", blobName)
			assert.Equal(t, expected[blobName], string(decoded), "data mismatch for %s", blobName)
			assert.Empty(t, r["filePath"], "filePath should be empty for inline mode")
		}

		// Test partial failure: one exists, one doesn't.
		mixedItems := []map[string]string{
			{"blobName": blobNames[0]},
			{"blobName": "inlineget/nonexistent.txt"},
		}
		mixedPayload := map[string]interface{}{
			"items": mixedItems,
		}
		mixedData, marshalErr := json.Marshal(mixedPayload)
		require.NoError(t, marshalErr)

		mixedOut, mixedErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkGet",
			Data:      mixedData,
		})
		require.NoError(t, mixedErr)

		var mixedResults []map[string]interface{}
		require.NoError(t, json.Unmarshal(mixedOut.Data, &mixedResults))
		require.Len(t, mixedResults, 2)
		for _, r := range mixedResults {
			if r["blobName"] == blobNames[0] {
				assert.Empty(t, r["error"])
				dataB64, ok := r["data"].(string)
				require.True(t, ok, "data should be a base64 string")
				decoded, decErr := base64.StdEncoding.DecodeString(dataB64)
				require.NoError(t, decErr, "failed to decode base64 data")
				assert.Equal(t, contents[0], string(decoded))
			} else {
				assert.NotEmpty(t, r["error"], "expected error for non-existent blob")
			}
		}

		// Cleanup.
		deletePayload := map[string]interface{}{
			"blobNames": blobNames,
		}
		deleteData, marshalErr := json.Marshal(deletePayload)
		require.NoError(t, marshalErr)
		_, cleanupErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkDelete",
			Data:      deleteData,
		})
		require.NoError(t, cleanupErr, "cleanup bulkDelete failed")

		return nil
	}

	testBulkCreateWithContentType := func(ctx flow.Context) error {
		// Tests bulkCreate with per-item contentType.
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		blobName := "contenttype/test.json"
		contentType := "application/json"
		data := `{"key":"value"}`

		items := []map[string]interface{}{
			{
				"blobName":    blobName,
				"data":        data,
				"contentType": contentType,
			},
		}
		createPayload := map[string]interface{}{
			"items": items,
		}
		createData, marshalErr := json.Marshal(createPayload)
		require.NoError(t, marshalErr)

		_, invokeCreateErr := client.InvokeBinding(ctx, &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "bulkCreate",
			Data:      createData,
		})
		require.NoError(t, invokeCreateErr)

		// Verify content type via list with metadata.
		listOut, listErr := listBlobRequest(ctx, client, "contenttype/", "", -1, false, false, false, false, false)
		require.NoError(t, listErr)

		var listItems []map[string]interface{}
		require.NoError(t, json.Unmarshal(listOut.Data, &listItems))
		require.Len(t, listItems, 1)
		props, ok := listItems[0]["Properties"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, contentType, props["ContentType"])

		// Cleanup.
		_, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
		require.NoError(t, invokeDeleteErr)

		return nil
	}

	flow.New(t, "blobstorage binding authentication using service principal").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/serviceprincipal"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			)...,
		)).
		Step("Create blob", testCreateGetListDelete).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "blobstorage binding main test suite with access key authentication").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/accesskey"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			)...,
		)).
		Step("Create blob", testCreateGetListDelete).
		Step("Create blob from file", testCreateBlobFromFile(false)).
		Step("List contents", testListContents).
		Step("Create blob with conflicting filename", testCreateBlobWithFileNameConflict).
		Step("List contents with various options", testListContentsWithOptions).
		Step("Creating a public blob does not work", testCreatePublicBlob(false, "")).
		Step("Create blob with invalid content hash", testCreateBlobInvalidContentHash).
		Step("Test snapshot deletion and listing", testSnapshotDeleteAndList).
		Step("Bulk create, get, and delete", testBulkCreateGetDelete).
		Step("Bulk get by prefix", testBulkGetByPrefix).
		Step("Get to file", testGetToFile).
		Step("Bulk create with plain encoding", testBulkCreatePlainEncoding).
		Step("Bulk create with inline data", testBulkCreateInlineData).
		Step("Bulk get inline data", testBulkGetInlineData).
		Step("Bulk create with content type", testBulkCreateWithContentType).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "decode base64 option for binary blobs with access key authentication").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/decodeBase64"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			)...,
		)).
		Step("Create blob from file", testCreateBlobFromFile(true)).
		Step("Bulk create with base64 decoding", testBulkCreateWithBase64).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "Blob Container Access Policy: Blog - with access key authentication").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/publicAccessBlob"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			)...,
		)).
		Step("Creating a public blob works", testCreatePublicBlob(true, "publiccontainer")).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	require.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "Blob Container Access Policy: Container - with access key authentication").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/publicAccessContainer"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGRPCPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			)...,
		)).
		Step("Creating a public blob works", testCreatePublicBlob(true, "alsopubliccontainer")).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(blobstorage.NewAzureBlobStorage, "azure.blobstorage")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
