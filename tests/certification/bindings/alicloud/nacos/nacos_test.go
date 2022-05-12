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

package nacosbinding_test

import (
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	nacosbinding "github.com/dapr/components-contrib/bindings/alicloud/nacos"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"

	nacosclient "github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

const (
	sidecarName = "nacos-sidecar"
	configData  = "my config data"
	bindingName = "alicloud-nacos-binding"
)

func createConfigAndData() (map[string]interface{}, map[string]string) {
	config := map[string]string{
		"config-id":    "123abc456def",
		"config-group": "test-group",
	}
	//Another way of create serverConfigs
	serverConfigs := []constant.ServerConfig{
		*constant.NewServerConfig(
			"localhost",
			8848,
			constant.WithScheme("http"),
			constant.WithContextPath("/nacos"),
		),
	}

	nacosConfig := map[string]interface{}{
		constant.KEY_SERVER_CONFIGS: serverConfigs,
	}
	return nacosConfig, config
}

func TestNacosBinding(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	invokeCreateWithConfig := func(ctx flow.Context, config map[string]string) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.CreateOperation),
			Data:      []byte(configData),
			Metadata:  config,
		}

		err := client.InvokeOutputBinding(ctx, invokeRequest)
		return err
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
		return rsp.Data, err
	}

	testInvokeCreateAndVerify := func(ctx flow.Context) error {
		nacosConfig, config := createConfigAndData()
		invokeErr := invokeCreateWithConfig(ctx, config)
		assert.NoError(t, invokeErr)

		// sleep to avoid metdata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		client, creatConfigErr := nacosclient.CreateConfigClient(nacosConfig)
		assert.NoError(t, creatConfigErr)
		content, getConfigError := client.GetConfig(vo.ConfigParam{
			DataId:   config["config-id"],
			Group:    config["config-group"],
			Content:  "",
			OnChange: nil,
		})
		assert.NoError(t, getConfigError)
		assert.Equal(t, configData, content)

		// cleanup
		_, err := client.DeleteConfig(vo.ConfigParam{
			DataId:   config["config-id"],
			Group:    config["config-group"],
			Content:  "",
			OnChange: nil,
		})
		assert.NoError(t, err)
		return nil
	}

	testInvokeGetAndVerify := func(ctx flow.Context) error {
		nacosConfig, config := createConfigAndData()

		// sleep to avoid metdata request rate limit before initializing new client
		flow.Sleep(3 * time.Second)

		client, creatConfigErr := nacosclient.CreateConfigClient(nacosConfig)
		assert.NoError(t, creatConfigErr)
		ok, getConfigError := client.PublishConfig(vo.ConfigParam{
			DataId:   config["config-id"],
			Group:    config["config-group"],
			Content:  configData,
			OnChange: nil,
		})
		assert.NoError(t, getConfigError)
		assert.True(t, ok)

		data, invokeErr := invokeGetWithConfig(ctx, config)
		assert.Equal(t, configData, string(data))
		assert.NoError(t, invokeErr)

		// cleanup
		_, err := client.DeleteConfig(vo.ConfigParam{
			DataId:   config["config-id"],
			Group:    config["config-group"],
			Content:  "",
			OnChange: nil,
		})
		assert.NoError(t, err)
		return nil
	}

	flow.New(t, "nacos binding create config").
		//Step(dockercompose.Run(nacosClusterName, dockerComposeYAML)).
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithInputBindings(
				bindings_loader.NewInput("alicloud.nacos", func() bindings.InputBinding {
					return nacosbinding.NewNacos(log)
				}),
			),
			runtime.WithOutputBindings(
				bindings_loader.NewOutput("alicloud.nacos", func() bindings.OutputBinding {
					return nacosbinding.NewNacos(log)
				}),
			))).
		Step("verify data sent to output binding is written to nacos", testInvokeCreateAndVerify).
		Step("verify data sent in nacos can be got correctly", testInvokeGetAndVerify).
		Run()
}
