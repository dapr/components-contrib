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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

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
	sidecarName = "blobstorage-sidecar"
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
	includeOptions["Snapshots"] = includeSnapshots
	includeOptions["UncommittedBlobs"] = includeUncommittedBlobs
	includeOptions["Copy"] = includeCopy
	includeOptions["Deleted"] = includeDeleted
	includeOptions["Metadata"] = includeMetadata
	requestOptions["Include"] = includeOptions

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
	assert.NoError(t, err)

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
		assert.NoError(t, invokeCreateErr)

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

		assert.NoError(t, invokeCreateErr2)

		res2, _ := getBlobRequest(ctx, client, blobName, false)
		newString := string(res2.Data)

		assert.NotEqual(t, oldString, newString)
		assert.Equal(t, newString, input2)

		// cleanup.
		out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), bloberror.BlobNotFound)

		// deleting the key again should fail.
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, blobName, nil)
		assert.Error(t, invokeDeleteErr2)
		assert.Contains(t, invokeDeleteErr2.Error(), bloberror.BlobNotFound)

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
			assert.NoError(t, invokeCreateErr)

			blobName := out.Metadata["blobName"]

			out, invokeGetErr := getBlobRequest(ctx, client, blobName, false)
			assert.NoError(t, invokeGetErr)
			responseData := out.Data
			if isBase64 {
				// input was automatically base64 decoded.
				// for comparison we will base64 encode the response data.
				responseData = []byte(base64.StdEncoding.EncodeToString(out.Data))
			}
			assert.Equal(t, responseData, dataBytes)
			assert.Empty(t, out.Metadata)

			out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, nil)
			assert.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion.
			_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
			assert.Error(t, invokeSecondGetErr)
			assert.Contains(t, invokeSecondGetErr.Error(), bloberror.BlobNotFound)

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
			assert.NoError(t, invokeCreateErr)

			blobName := out.Metadata["blobName"]
			storageAccountName := os.Getenv("AzureBlobStorageAccount")
			if containerName == "" {
				containerName = os.Getenv("AzureBlobStorageContainer")
			}

			// verify the blob is public via http request.
			url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", storageAccountName, containerName, blobName)
			resp, httpErr := http.Get(url) //nolint:gosec
			assert.NoError(t, httpErr)
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
			assert.NoError(t, invokeDeleteErr)

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

		assert.NoError(t, invokeCreateErr)

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
		assert.NoError(t, invokeGetErr)
		assert.Equal(t, input, string(out.Data))
		assert.Contains(t, out.Metadata, "Custom")
		assert.Equal(t, "hello-world", out.Metadata["Custom"])

		out, invokeErr := listBlobRequest(ctx, client, "", "", -1, true, false, false, false, false)
		assert.NoError(t, invokeErr)
		var output []map[string]interface{}
		unmarshalErr := json.Unmarshal(out.Data, &output)
		assert.NoError(t, unmarshalErr)

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
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getBlobRequest(ctx, client, "filename.txt", false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), bloberror.BlobNotFound)

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
		assert.NoError(t, invokeErr)

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
		assert.NoError(t, invokeCreateErr1)

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
		assert.NoError(t, invokeCreateErr2)

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
		assert.NoError(t, invokeCreateErr3)

		// list the contents of the container.
		out, listErr := listBlobRequest(ctx, client, "prefixA", "", 1, false, false, false, false, false)
		assert.NoError(t, listErr)

		var output []map[string]interface{}
		unmarshalErr := json.Unmarshal(out.Data, &output)
		assert.NoError(t, unmarshalErr)

		assert.Equal(t, 1, len(output))
		assert.Contains(t, output[0]["Name"], "prefixA")

		nextMarker := out.Metadata["marker"]

		// list the contents of the container with a marker.
		out2, listErr2 := listBlobRequest(ctx, client, "prefixA", nextMarker, 1, false, false, false, false, false)
		assert.NoError(t, listErr2)

		var output2 []map[string]interface{}
		err2 := json.Unmarshal(out2.Data, &output2)
		assert.NoError(t, err2)

		assert.Equal(t, 1, len(output2))
		assert.Contains(t, output2[0]["Name"], "prefixA")

		// cleanup.
		_, invokeDeleteErr1 := deleteBlobRequest(ctx, client, "prefixA/filename.txt", nil)
		assert.NoError(t, invokeDeleteErr1)
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, "prefixAfilename.txt", nil)
		assert.NoError(t, invokeDeleteErr2)
		_, invokeDeleteErr3 := deleteBlobRequest(ctx, client, "prefixB/filename.txt", nil)
		assert.NoError(t, invokeDeleteErr3)

		// list deleted items with prefix.
		out3, listErr3 := listBlobRequest(ctx, client, "prefixA/", "", -1, false, false, false, false, true)
		assert.NoError(t, listErr3)

		// this will only return the deleted items if soft delete policy is enabled for the blob service.
		assert.Equal(t, "1", out3.Metadata["number"])
		var output3 []map[string]interface{}
		err3 := json.Unmarshal(out3.Data, &output3)
		assert.NoError(t, err3)
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
		assert.NoError(t, uploadErr)
		_, createSnapshotErr := blobClient.CreateSnapshot(
			ctx, &blob.CreateSnapshotOptions{}) //nolint:exhaustivestruct
		assert.NoError(t, createSnapshotErr)

		// list the contents of the container including snapshots for the specific blob only.
		out, listErr := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		assert.NoError(t, listErr)
		assert.Equal(t, out.Metadata["number"], "2")

		// delete snapshots.
		_, invokeDeleteErr := deleteBlobRequest(ctx, client, "snapshotthis.txt", ptr.Of(string(blob.DeleteSnapshotsOptionTypeOnly)))
		assert.NoError(t, invokeDeleteErr)

		// verify snapshot is deleted.
		out2, listErr2 := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		assert.NoError(t, listErr2)
		assert.Equal(t, "1", out2.Metadata["number"])

		// create another snapshot.
		_, createSnapshotErr2 := blobClient.CreateSnapshot(
			ctx, &blob.CreateSnapshotOptions{}) //nolint:exhaustivestruct
		assert.NoError(t, createSnapshotErr2)

		// delete base blob and snapshots all at once.
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, "snapshotthis.txt", ptr.Of(string(blob.DeleteSnapshotsOptionTypeInclude)))
		assert.NoError(t, invokeDeleteErr2)

		// verify base blob and snapshots are deleted.
		out3, listErr3 := listBlobRequest(ctx, client, "snapshotthis.txt", "", -1, false, true, false, false, false)
		assert.NoError(t, listErr3)
		assert.Equal(t, "0", out3.Metadata["number"])

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
	assert.NoError(t, err)

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
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

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
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

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
	assert.NoError(t, err)

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
