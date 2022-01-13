// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureblobstoragebinding_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/azure/blobstorage"
	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
)

const (
	sidecarName = "blobstorage-sidecar"
)

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
	return out, invokeGetErr
}

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
		return nil, marshalErr
	}

	invokeRequest := &daprsdk.InvokeBindingRequest{
		Name:      "azure-blobstorage-output",
		Operation: "list",
		Data:      optionsBytes,
		Metadata:  nil,
	}

	out, invokeErr := client.InvokeBinding(ctx, invokeRequest)
	return out, invokeErr
}

func deleteBlobRequest(ctx flow.Context, client daprsdk.Client, name string, deleteSnapshotsOption string) (out *daprsdk.BindingEvent, err error) {
	invokeDeleteMetadata := map[string]string{
		"blobName":        name,
		"deleteSnapshots": deleteSnapshotsOption,
	}

	invokeGetRequest := &daprsdk.InvokeBindingRequest{
		Name:      "azure-blobstorage-output",
		Operation: "delete",
		Data:      nil,
		Metadata:  invokeDeleteMetadata,
	}

	out, invokeDeleteErr := client.InvokeBinding(ctx, invokeGetRequest)
	return out, invokeDeleteErr
}

func TestBlobStorage(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]

	log := logger.NewLogger("dapr.components")

	testCreateBlobWithFileNameConflict := func(ctx flow.Context) error {
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

		// cleanup
		out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, "")
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion
		_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), "ServiceCode=BlobNotFound")

		// deleting the key again should fail
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, blobName, "")
		assert.Error(t, invokeDeleteErr2)
		assert.Contains(t, invokeDeleteErr2.Error(), "ServiceCode=BlobNotFound")

		return nil
	}

	testCreateBlobInvalidContentHash := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)
		wrongBytesForContentHash := []byte("wrong content to hash")
		h := md5.New()
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
		assert.Contains(t, invokeCreateErr.Error(), "ServiceCode=Md5Mismatch")

		return nil
	}

	testCreateBlobFromFile := func(isBase64 bool) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			dataBytes, err := os.ReadFile("dapr.svg")
			if isBase64 {
				dataBytes = []byte(base64.StdEncoding.EncodeToString(dataBytes))
			}

			assert.NoError(t, err)

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
				// input was automatically base64 decoded
				// for comparison we will base64 encode the response data
				responseData = []byte(base64.StdEncoding.EncodeToString(out.Data))
			}
			assert.Equal(t, responseData, dataBytes)
			assert.Empty(t, out.Metadata)

			out, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, "")
			assert.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion
			_, invokeSecondGetErr := getBlobRequest(ctx, client, blobName, false)
			assert.Error(t, invokeSecondGetErr)
			assert.Contains(t, invokeSecondGetErr.Error(), "ServiceCode=BlobNotFound")

			return nil
		}
	}

	testCreatePublicBlob := func(shoudBePublic bool, containerName string) func(ctx flow.Context) error {
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

			// verify the blob is public via http request
			url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", storageAccountName, containerName, blobName)
			resp, err := http.Get(url)
			assert.NoError(t, err)
			body, _ := ioutil.ReadAll(resp.Body)

			if shoudBePublic {
				assert.Less(t, resp.StatusCode, 400)
				assert.Equal(t, inputBytes, body)
			} else {
				assert.Greater(t, resp.StatusCode, 399)
			}

			// cleanup
			_, invokeDeleteErr := deleteBlobRequest(ctx, client, blobName, "")
			assert.NoError(t, invokeDeleteErr)

			return nil
		}
	}

	testCreateGetListDelete := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)
		h := md5.New()
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
		assert.Equal(t, string(out.Data), input)
		assert.Contains(t, out.Metadata, "custom")
		assert.Equal(t, out.Metadata["custom"], "hello-world")

		out, invokeErr := listBlobRequest(ctx, client, "", "", -1, true, false, false, false, false)
		assert.NoError(t, invokeErr)
		var output []map[string]interface{}
		err := json.Unmarshal(out.Data, &output)
		assert.NoError(t, err)

		found := false
		for _, item := range output {
			if item["Name"] == "filename.txt" {
				found = true
				log.Errorf("found item: %v", item)
				properties := item["Properties"].(map[string]interface{})
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

		out, invokeDeleteErr := deleteBlobRequest(ctx, client, "filename.txt", "")
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion
		_, invokeSecondGetErr := getBlobRequest(ctx, client, "filename.txt", false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), "ServiceCode=BlobNotFound")

		return nil
	}

	testListContents := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		_, invokeErr := listBlobRequest(ctx, client, "", "", 0, false, false, false, false, false)
		assert.NoError(t, invokeErr)

		return invokeErr
	}

	testListContentsWithOptions := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		// create a blob with a prefix of "prefixA"
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

		// create another blob with a prefix of "prefixA"
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

		// create a blob with a prefix of "prefixB"
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

		// list the contents of the container
		out, listErr := listBlobRequest(ctx, client, "prefixA", "", 1, false, false, false, false, false)
		assert.NoError(t, listErr)

		var output []map[string]interface{}
		err := json.Unmarshal(out.Data, &output)
		assert.NoError(t, err)

		assert.Equal(t, len(output), 1)
		assert.Equal(t, output[0]["Name"], "prefixA/filename.txt")

		log.Error(output)
		nextMarker := out.Metadata["marker"]
		fmt.Println("nextMarker: ", nextMarker)

		// list the contents of the container with a marker
		out2, listErr2 := listBlobRequest(ctx, client, "prefixA", nextMarker, 1, false, false, false, false, false)
		assert.NoError(t, listErr2)

		var output2 []map[string]interface{}
		err2 := json.Unmarshal(out2.Data, &output2)
		assert.NoError(t, err2)

		assert.Equal(t, len(output2), 1)
		assert.Equal(t, output2[0]["Name"], "prefixAfilename.txt")

		// cleanup
		_, invokeDeleteErr1 := deleteBlobRequest(ctx, client, "prefixA/filename.txt", "")
		assert.NoError(t, invokeDeleteErr1)
		_, invokeDeleteErr2 := deleteBlobRequest(ctx, client, "prefixAfilename.txt", "")
		assert.NoError(t, invokeDeleteErr2)
		_, invokeDeleteErr3 := deleteBlobRequest(ctx, client, "prefixB/filename.txt", "")
		assert.NoError(t, invokeDeleteErr3)

		// list deleted items with prefix
		out3, listErr3 := listBlobRequest(ctx, client, "prefixA", "", -1, false, false, false, false, true)
		assert.NoError(t, listErr3)

		// this will only return the deleted items if soft delete policy is enabled for the blob service
		assert.Equal(t, out3.Metadata["number"], "2")
		var output3 []map[string]interface{}
		err3 := json.Unmarshal(out3.Data, &output3)
		assert.NoError(t, err3)
		assert.Equal(t, len(output3), 2)

		// include snapshots

		out4, listErr4 := listBlobRequest(ctx, client, "prefixA", "", -1, false, true, false, false, true)
		assert.NoError(t, listErr4)
		snapshotCountBefore, _ := strconv.Atoi(out4.Metadata["number"])

		// overwriting the deleted blob should create a snapshot of the soft deleted blob
		// create a blob with a prefix of "prefixA"
		invokeCreateMetadata4 := map[string]string{
			"blobName": "prefixA/filename.txt",
		}

		invokeCreateRequest4 := &daprsdk.InvokeBindingRequest{
			Name:      "azure-blobstorage-output",
			Operation: "create",
			Data:      []byte("some overwritten content"),
			Metadata:  invokeCreateMetadata4,
		}

		_, invokeCreateErr4 := client.InvokeBinding(ctx, invokeCreateRequest4)
		assert.NoError(t, invokeCreateErr4)

		out5, listErr5 := listBlobRequest(ctx, client, "prefixA", "", -1, false, true, false, false, true)
		assert.NoError(t, listErr5)

		// this will only return the deleted items if soft delete policy is enabled for the blob service
		snapshotCountAfter, _ := strconv.Atoi(out5.Metadata["number"])
		assert.Greater(t, snapshotCountAfter, snapshotCountBefore)
		var output5 []map[string]interface{}
		err5 := json.Unmarshal(out5.Data, &output5)
		assert.NoError(t, err5)
		assert.Greater(t, len(output5), 2)
		assert.Greater(t, len(output5), snapshotCountBefore)

		// cleanup
		_, invokeDeleteErr4 := deleteBlobRequest(ctx, client, "prefixA/filename.txt", "")
		assert.NoError(t, invokeDeleteErr4)

		// no need to test the other operations here as they

		return nil
	}

	flow.New(t, "blobstorage binding authentication using service principal").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/serviceprincipal"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Create blob", testCreateGetListDelete).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "blobstorage binding authentication using access key").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/accesskey"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Create blob", testCreateGetListDelete).
		Step("Create blob from file", testCreateBlobFromFile(false)).
		Step("List contents", testListContents).
		Step("Create blob with conflicting filename", testCreateBlobWithFileNameConflict).
		Step("List contents with prefix", testListContentsWithOptions).
		Step("Creating a public blob does not work", testCreatePublicBlob(false, "")).
		Step("Create blob with invalid content hash", testCreateBlobInvalidContentHash).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "decode base64 option for binary blobs").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/decodeBase64"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Create blob from file", testCreateBlobFromFile(true)).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "blobstorage binding authentication using access key").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/publicAccessBlob"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Creating a public blob works", testCreatePublicBlob(true, "publiccontainer")).
		Run()

	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort = ports[0]
	currentHTTPPort = ports[1]

	flow.New(t, "blobstorage binding authentication using access key").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/publicAccessContainer"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("azure.blobstorage", func() bindings.OutputBinding {
					return blobstorage.NewAzureBlobStorage(log)
				}),
			))).
		Step("Creating a public blob works", testCreatePublicBlob(true, "alsopubliccontainer")).
		Run()
}
