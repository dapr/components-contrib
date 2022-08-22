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

package localstorage_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	bindings_localstorage "github.com/dapr/components-contrib/bindings/localstorage"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	sidecarName = "localstorage-sidecar"
	bindingName = "localstorage-binding"
	dir         = "./tmp"
	fileName    = "test.txt"
	fileData    = "test data"
)

func TestLocalStorage(t *testing.T) {
	logger := logger.NewLogger("dapr.components")

	invokeCreateWithConfig := func(ctx flow.Context, config map[string]string) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte(fileData),
			Metadata:  config,
		}

		return client.InvokeOutputBinding(ctx, invokeRequest)
	}

	invokeGetWithConfig := func(ctx flow.Context, config map[string]string) ([]byte, error) {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  config,
		}

		rsp, err := client.InvokeBinding(ctx, invokeRequest)
		if err != nil {
			return nil, err
		}
		return rsp.Data, err
	}

	invokeDeleteWithConfig := func(ctx flow.Context, config map[string]string) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.DeleteOperation),
			Metadata:  config,
		}

		return client.InvokeOutputBinding(ctx, invokeRequest)
	}

	invokeListWithConfig := func(ctx flow.Context, config map[string]string) ([]byte, error) {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.ListOperation),
			Metadata:  config,
		}

		rsp, err := client.InvokeBinding(ctx, invokeRequest)
		if err != nil {
			return nil, err
		}
		return rsp.Data, err
	}

	testInvokeCreateAndGet := func(ctx flow.Context) error {
		// sleep to avoid metadata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		err := invokeCreateWithConfig(ctx, map[string]string{"fileName": fileName})
		assert.NoError(t, err)

		raw, err := invokeGetWithConfig(ctx, map[string]string{"fileName": fileName})
		assert.NoError(t, err)
		assert.Equal(t, fileData, string(raw))

		return clear()
	}

	testInvokeGetFileNotExists := func(ctx flow.Context) error {
		// sleep to avoid metadata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		_, err := invokeGetWithConfig(ctx, map[string]string{"fileName": "not-exists.txt"})
		assert.Error(t, err)

		return clear()
	}

	testInvokeDeleteFile := func(ctx flow.Context) error {
		// sleep to avoid metadata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		err := invokeCreateWithConfig(ctx, map[string]string{"fileName": fileName})
		assert.NoError(t, err)

		err = invokeDeleteWithConfig(ctx, map[string]string{"fileName": fileName})
		assert.NoError(t, err)

		return clear()
	}

	testInvokeListFiles := func(ctx flow.Context) error {
		// sleep to avoid metadata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		fileNames := []string{fileName + "1", fileName + "2", fileName + "3"}
		for _, fileName := range fileNames {
			err := invokeCreateWithConfig(ctx, map[string]string{"fileName": fileName})
			assert.NoError(t, err)
		}

		raw, err := invokeListWithConfig(ctx, nil)
		assert.NoError(t, err)
		for _, fileName := range fileNames {
			assert.Contains(t, string(raw), fileName)
		}

		// clear
		return clear()
	}

	flow.New(t, "test local storage operations").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("localstorage", func() bindings.OutputBinding {
					return bindings_localstorage.NewLocalStorage(logger)
				}),
			))).
		Step("create file and get data success", testInvokeCreateAndGet).
		Step("get error when file not exists", testInvokeGetFileNotExists).
		Step("delete file success", testInvokeDeleteFile).
		Step("list files", testInvokeListFiles).
		Run()
}

func clear() error {
	return os.RemoveAll(dir)
}
