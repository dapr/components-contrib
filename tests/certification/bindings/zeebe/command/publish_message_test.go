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

package command_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
)

func TestPublishMessage(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	publishMessageOnlyWithMessageName := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)

		return nil
	}

	publishMessageWithCorrelationId := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"messageName":    "some-message",
			"correlationKey": "some-correlation-key",
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)

		return nil
	}

	publishMessageWithTimeToLive := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
			"timeToLive":  "5m",
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)

		return nil
	}

	publishMessageWithVariables := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)

		return nil
	}

	publishMessageWithAllParams := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"messageName":    "some-message",
			"correlationKey": "some-correlation-key",
			"timeToLive":     "5m",
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)

		return nil
	}

	flow.New(t, "Test fail job operation").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Publish a message only with message name", publishMessageOnlyWithMessageName).
		Step("publish a message with a correlation ID", publishMessageWithCorrelationId).
		Step("publish a message with a time to live", publishMessageWithTimeToLive).
		Step("publish a message with variables", publishMessageWithVariables).
		Step("publish a message with all params", publishMessageWithAllParams).
		Run()
}
