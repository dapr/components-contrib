/*
Copyright 2023 The Dapr Authors
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

package awss3binding_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"

	"testing"

	bindings_s3 "github.com/dapr/components-contrib/bindings/aws/s3"
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

	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	sidecarName          = "bindings-s3-sidecar"
	bindingsMetadataName = "s3-cert-tests"
)

var bucketName = "bucketName"

func init() {
	if envVal := os.Getenv("BINDINGS_AWS_S3_BUCKET"); envVal != "" {
		bucketName = envVal
	}
}

func TestAWSS3CertificationTests(t *testing.T) {
	defer teardown(t)

	t.Run("S3SBasic", func(t *testing.T) {
		S3SBasic(t)
	})

	t.Run("S3SForcePathStyle", func(t *testing.T) {
		S3SForcePathStyle(t)
	})

	t.Run("S3SBase64", func(t *testing.T) {
		S3SBase64(t)
	})
}

// createObjectRequest is used to make a common binding request for create operation.
func createObjectRequest(ctx flow.Context, client daprsdk.Client, dataBytes []byte, invokeCreateMetadata map[string]string) (*daprsdk.BindingEvent, error) {
	invokeCreateRequest := &daprsdk.InvokeBindingRequest{
		Name:      bindingsMetadataName,
		Operation: "create",
		Data:      dataBytes,
		Metadata:  invokeCreateMetadata,
	}

	return client.InvokeBinding(ctx, invokeCreateRequest)
}

// listObjectRequest is used to make a common binding request for the list operation.
func listObjectRequest(ctx flow.Context, client daprsdk.Client) (out *daprsdk.BindingEvent, err error) {
	invokeRequest := &daprsdk.InvokeBindingRequest{
		Name:      bindingsMetadataName,
		Operation: "list",
		Data:      nil,
		Metadata:  nil,
	}

	out, invokeErr := client.InvokeBinding(ctx, invokeRequest)
	if invokeErr != nil {
		return nil, fmt.Errorf("%w", invokeErr)
	}
	return out, nil
}

// getObjectRequest is used to make a common binding request for the get operation.
func getObjectRequest(ctx flow.Context, client daprsdk.Client, name string, isBase64 bool) (out *daprsdk.BindingEvent, err error) {
	invokeGetMetadata := map[string]string{
		"key":          name,
		"encodeBase64": fmt.Sprintf("%t", isBase64),
	}

	return getObjectRequestWithMetadata(ctx, client, invokeGetMetadata)

}

// getObjectRequest is used to make a common binding request for the get operation passing metadata.
func getObjectRequestWithMetadata(ctx flow.Context, client daprsdk.Client, invokeGetMetadata map[string]string) (out *daprsdk.BindingEvent, err error) {
	invokeGetRequest := &daprsdk.InvokeBindingRequest{
		Name:      bindingsMetadataName,
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

// deleteObjectRequest is used to make a common binding request for the delete operation.
func deleteObjectRequest(ctx flow.Context, client daprsdk.Client, name string) (out *daprsdk.BindingEvent, err error) {
	invokeDeleteMetadata := map[string]string{
		"key": name,
	}

	invokeGetRequest := &daprsdk.InvokeBindingRequest{
		Name:      bindingsMetadataName,
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

// Verify S3 Basic Binding Support (Create, Get, List, Delete)
func S3SBasic(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]
	objectName := "filename.txt"

	testCreateGetListDelete := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		input := "some example content"
		dataBytes := []byte(input)

		invokeCreateMetadata := map[string]string{
			"key": objectName,
		}

		_, invokeCreateErr := createObjectRequest(ctx, client, dataBytes, invokeCreateMetadata)
		assert.NoError(t, invokeCreateErr)

		invokeGetMetadata := map[string]string{
			"key": objectName,
		}

		invokeGetRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingsMetadataName,
			Operation: "get",
			Data:      nil,
			Metadata:  invokeGetMetadata,
		}

		out, invokeGetErr := client.InvokeBinding(ctx, invokeGetRequest)
		assert.NoError(t, invokeGetErr)
		assert.Equal(t, input, string(out.Data))

		out, invokeErr := listObjectRequest(ctx, client)
		assert.NoError(t, invokeErr)
		var output s3.ListObjectsOutput
		unmarshalErr := json.Unmarshal(out.Data, &output)
		assert.NoError(t, unmarshalErr)

		found := false
		for _, item := range output.Contents {
			if *item.Key == objectName {
				found = true
				break
			}
		}
		assert.True(t, found)

		out, invokeDeleteErr := deleteObjectRequest(ctx, client, objectName)
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getObjectRequest(ctx, client, objectName, false)
		assert.Error(t, invokeSecondGetErr)
		assert.Contains(t, invokeSecondGetErr.Error(), "error downloading S3 object")

		return nil
	}

	flow.New(t, "AWS S3 binding basic").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/basic"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("Create/Get/List/Delete S3 Object", testCreateGetListDelete).
		Run()

}

// Verify forcePathStyle
func S3SForcePathStyle(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]
	objectName := "filename.txt"
	locationForcePathStyleFalse := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", bucketName, objectName)
	locationForcePathStyleTrue := fmt.Sprintf("https://s3.amazonaws.com/%s/%s", bucketName, objectName)

	testForcePathStyle := func(forcePathStyle string) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			input := "some example content"
			dataBytes := []byte(input)

			invokeCreateMetadata := map[string]string{
				"key": objectName,
			}

			cout, invokeCreateErr := createObjectRequest(ctx, client, dataBytes, invokeCreateMetadata)
			assert.NoError(t, invokeCreateErr)
			var createResponse struct {
				Location   string  `json:"location"`
				VersionID  *string `json:"versionID"`
				PresignURL string  `json:"presignURL,omitempty"`
			}
			unmarshalErr := json.Unmarshal(cout.Data, &createResponse)
			assert.NoError(t, unmarshalErr)
			assert.Equal(t, createResponse.Location, forcePathStyle)

			out, invokeDeleteErr := deleteObjectRequest(ctx, client, objectName)
			assert.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion.
			_, invokeSecondGetErr := getObjectRequest(ctx, client, objectName, false)
			assert.Error(t, invokeSecondGetErr)
			assert.Contains(t, invokeSecondGetErr.Error(), "error downloading S3 object")

			return nil
		}
	}

	flow.New(t, "AWS S3 binding with forcePathStyle True").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/forcePathStyleTrue"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("Create/Delete S3 Object forcePathStyle True", testForcePathStyle(locationForcePathStyleTrue)).
		Run()

	flow.New(t, "AWS S3 binding with forcePathStyleFalse").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/forcePathStyleFalse"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("Create/Delete S3 Object forcePathStyle False", testForcePathStyle(locationForcePathStyleFalse)).
		Run()
}

// Verify Base64 (Encode/Decode)
func S3SBase64(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGRPCPort := ports[0]
	currentHTTPPort := ports[1]

	testCreateBase64FromFile := func() func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			dataBytes := []byte(base64.StdEncoding.EncodeToString([]byte("somecontent")))
			invokeCreateMetadata := map[string]string{
				"decodeBase64": "true",
			}

			out, invokeCreateErr := createObjectRequest(ctx, client, dataBytes, invokeCreateMetadata)
			assert.NoError(t, invokeCreateErr)

			genKey := out.Metadata["key"]
			isBase64 := true
			out, invokeGetErr := getObjectRequest(ctx, client, genKey, isBase64)
			assert.NoError(t, invokeGetErr)
			assert.Equal(t, out.Data, dataBytes)
			assert.Empty(t, out.Metadata)

			out, invokeDeleteErr := deleteObjectRequest(ctx, client, genKey)
			assert.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion.
			_, invokeSecondGetErr := getObjectRequest(ctx, client, genKey, false)
			assert.Error(t, invokeSecondGetErr)

			return nil
		}
	}

	testCreateFromFileGetEncodeBase64 := func() func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(currentGRPCPort))
			if clientErr != nil {
				panic(clientErr)
			}
			defer client.Close()

			dataBytes := []byte("somecontent not base64 encoded")
			b64EncodedDataBytes := []byte(base64.StdEncoding.EncodeToString(dataBytes))
			invokeCreateMetadata := map[string]string{}

			out, invokeCreateErr := createObjectRequest(ctx, client, dataBytes, invokeCreateMetadata)
			assert.NoError(t, invokeCreateErr)

			genKey := out.Metadata["key"]
			invokeGetMetadata := map[string]string{
				"key": genKey,
			}
			out, invokeGetErr := getObjectRequestWithMetadata(ctx, client, invokeGetMetadata)
			assert.NoError(t, invokeGetErr)
			assert.Equal(t, out.Data, b64EncodedDataBytes)
			assert.Empty(t, out.Metadata)

			out, invokeDeleteErr := deleteObjectRequest(ctx, client, genKey)
			assert.NoError(t, invokeDeleteErr)
			assert.Empty(t, out.Data)

			// confirm the deletion.
			_, invokeSecondGetErr := getObjectRequest(ctx, client, genKey, false)
			assert.Error(t, invokeSecondGetErr)

			return nil
		}
	}

	flow.New(t, "decode base64 option for binary").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/decodeBase64"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("Create blob from file", testCreateBase64FromFile()).
		Run()

	flow.New(t, "upload regular file get as encode base64").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/encodeBase64"),
			embedded.WithDaprGRPCPort(currentGRPCPort),
			embedded.WithDaprHTTPPort(currentHTTPPort),
			componentRuntimeOptions(),
		)).
		Step("Create blob from file get  encode base64", testCreateFromFileGetEncodeBase64()).
		Run()

}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterOutputBinding(bindings_s3.NewAWSS3, "aws.s3")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []runtime.Option{
		runtime.WithBindings(bindingsRegistry),
		runtime.WithSecretStores(secretstoreRegistry),
	}
}

func teardown(t *testing.T) {
	t.Logf("AWS S3 Binding CertificationTests teardown...")
	//Dapr runtime automatically creates the following queues, topics
	//so here they get deleted.

	t.Logf("AWS S3 Binding CertificationTests teardown...done!")
}
