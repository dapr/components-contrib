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

package command_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

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

func TestCancelInstanceOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	id := zeebe_test.TestID()

	var processInstanceKey int64

	deployProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		deployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id))

		assert.NoError(t, err)
		assert.Equal(t, id, deployment.Deployments[0].Metadata.Process.BpmnProcessId)

		return nil
	}

	createProcessInstance := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, 0, processInstance.ProcessInstanceKey)

		processInstanceKey = processInstance.ProcessInstanceKey

		return nil
	}

	cancelProcessInstance := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"processInstanceKey": processInstanceKey,
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.CancelInstanceOperation, data, nil)
		assert.NoError(t, err)
		assert.Nil(t, res.Data)
		assert.Nil(t, res.Metadata)

		return nil
	}

	returnErrorForNotExistingProcessInstance := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"processInstanceKey": 0,
		})
		assert.NotNil(t, data)
		assert.NoError(t, err)

		_, err = zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.CancelInstanceOperation, data, nil)
		assert.Error(t, err)

		return nil
	}

	flow.New(t, "Test cancel instance operation").
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
		Step("Deploy process", deployProcess).
		Step("Create process instance", createProcessInstance).
		Step("Cancel a process instance", cancelProcessInstance).
		Step("Return error for not existing process instance", returnErrorForNotExistingProcessInstance).
		Run()
}
