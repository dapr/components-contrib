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

package kitex

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	bindings_kitex "github.com/dapr/components-contrib/bindings/kitex"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	kitex_e2e "github.com/dapr/components-contrib/tests/e2e/bindings/kitex"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	hostports   = "127.0.0.1:8888"
	destService = "echo"
	MethodName  = "echo"
	sidecarName = "kitex-sidecar"
	bindingName = "kitex-binding"
)

const (
	metadataRPCMethodName  = "methodName"
	metadataRPCDestService = "destService"
	metadataRPCHostports   = "hostPorts"
	metadataRPCVersion     = "version"
)

func TestKitexBinding(t *testing.T) {
	testKitexInvocation := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()
		codec := utils.NewThriftMessageCodec()

		req := &api.EchoEchoArgs{Req: &api.Request{Message: "hello dapr"}}

		reqData, err := codec.Encode(MethodName, thrift.CALL, 0, req)
		require.NoError(t, err)
		metadata := map[string]string{
			metadataRPCVersion:     kitex.Version,
			metadataRPCHostports:   hostports,
			metadataRPCDestService: destService,
			metadataRPCMethodName:  MethodName,
		}

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  metadata,
			Data:      reqData,
		}

		resp, err := client.InvokeBinding(ctx, invokeRequest)
		require.NoError(t, err)
		result := &api.EchoEchoResult{}

		_, _, err = codec.Decode(resp.Data, result)
		require.NoError(t, err)
		assert.Equal(t, "hello dapr,hi Kitex", result.Success.Message)
		return nil
	}

	go func() {
		err := kitex_e2e.EchoKitexServer()
		require.NoError(t, err)
	}()

	time.Sleep(time.Second * 3)

	flow.New(t, "test kitex binding config").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(strconv.Itoa(runtime.DefaultDaprAPIGRPCPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(runtime.DefaultDaprHTTPPort)),
			embedded.WithBindings(newBindingsRegistry()))).
		Step("verify kitex invocation", testKitexInvocation).
		Run()
}

func newBindingsRegistry() *bindings_loader.Registry {
	log := logger.NewLogger("dapr.components")

	r := bindings_loader.NewRegistry()
	r.Logger = log
	r.RegisterOutputBinding(bindings_kitex.NewKitexOutput, "kitex")
	return r
}
