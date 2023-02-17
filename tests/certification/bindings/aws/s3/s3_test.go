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
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"

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

func init() {

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

// listObejctRequest is used to make a common binding request for the list operation.
func listObejctRequest(ctx flow.Context, client daprsdk.Client) (out *daprsdk.BindingEvent, err error) {
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

// getObejctRequest is used to make a common binding request for the get operation.
func getObejctRequest(ctx flow.Context, client daprsdk.Client, name string, includeMetadata bool) (out *daprsdk.BindingEvent, err error) {
	invokeGetMetadata := map[string]string{
		"key": name,
	}

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

// deleteObejctRequest is used to make a common binding request for the delete operation.
func deleteObejctRequest(ctx flow.Context, client daprsdk.Client, name string) (out *daprsdk.BindingEvent, err error) {
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

		invokeCreateRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingsMetadataName,
			Operation: "create",
			Data:      dataBytes,
			Metadata:  invokeCreateMetadata,
		}

		_, invokeCreateErr := client.InvokeBinding(ctx, invokeCreateRequest)

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

		out, invokeErr := listObejctRequest(ctx, client)
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

		out, invokeDeleteErr := deleteObejctRequest(ctx, client, objectName)
		assert.NoError(t, invokeDeleteErr)
		assert.Empty(t, out.Data)

		// confirm the deletion.
		_, invokeSecondGetErr := getObejctRequest(ctx, client, objectName, false)
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

}

// Verify Base64 (Encode/Decode)
func S3SBase64(t *testing.T) {

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
