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
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/dapr/pkg/config/protocol"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/service/common"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
)

func TestCreateInstanceOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()

	variables := map[string]interface{}{
		"foo": "bar",
		"boo": "baz",
	}

	var version1ProcessDefinitionKey int64
	var version2ProcessDefinitionKey int64

	workers := func(timeout time.Duration) func(ctx flow.Context, s common.Service) error {
		return func(ctx flow.Context, s common.Service) error {
			return multierr.Combine(
				s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {

					time.Sleep(timeout) // To test timeouts

					return in.Data, nil
				}),
			)
		}
	}

	deployVersion1 := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		firstDeployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id))
		assert.NoError(t, err)
		assert.Equal(t, id, firstDeployment.Deployments[0].Metadata.Process.BpmnProcessId)
		assert.Equal(t, int32(1), firstDeployment.Deployments[0].Metadata.Process.Version)

		version1ProcessDefinitionKey = firstDeployment.Deployments[0].Metadata.Process.ProcessDefinitionKey

		return nil
	}

	deployVersion2 := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		secondDeployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id),
			// changing the name results in a new version
			zeebe_test.NameModifier(id))
		assert.NoError(t, err)
		assert.Equal(t, id, secondDeployment.Deployments[0].Metadata.Process.BpmnProcessId)
		assert.Equal(t, int32(2), secondDeployment.Deployments[0].Metadata.Process.Version)

		version2ProcessDefinitionKey = secondDeployment.Deployments[0].Metadata.Process.ProcessDefinitionKey

		return nil
	}

	testCreateInstanceByBpmnProcessIdVersion1 := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId":  id,
				"version":        1,
				"variables":      variables,
				"withResult":     withResult,
				"fetchVariables": []string{"foo"},
				"requestTimeout": requestTimeout,
			})
			assert.NoError(t, err)
			assert.Equal(t, version1ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
			assert.NotNil(t, processInstance.ProcessInstanceKey)
			assert.Equal(t, id, processInstance.BpmnProcessId)
			assert.Equal(t, int32(1), processInstance.Version)
			if withResult {
				assert.NotNil(t, processInstance.Variables)
				assert.Equal(t, "{\"foo\":\"bar\"}", processInstance.Variables)
			}

			return nil
		}
	}

	testCreateInstanceByBpmnProcessIdLatestVersion := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId":  id,
				"variables":      variables,
				"withResult":     withResult,
				"fetchVariables": []string{"foo"},
				"requestTimeout": requestTimeout,
			})
			assert.NoError(t, err)
			assert.Equal(t, version2ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
			assert.NotNil(t, processInstance.ProcessInstanceKey)
			assert.Equal(t, id, processInstance.BpmnProcessId)
			assert.Equal(t, int32(2), processInstance.Version)
			if withResult {
				assert.NotNil(t, processInstance.Variables)
				assert.Equal(t, "{\"foo\":\"bar\"}", processInstance.Variables)
			}

			return nil
		}
	}

	testCheckForExistingBpmnProcessId := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			_, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId":  "not-existing",
				"variables":      variables,
				"withResult":     withResult,
				"fetchVariables": []string{"foo"},
				"requestTimeout": requestTimeout,
			})
			assert.Error(t, err)

			return nil
		}
	}

	testCreateInstanceByProcessDefinitionKeyVersion1 := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"processDefinitionKey": version1ProcessDefinitionKey,
				"variables":            variables,
				"withResult":           withResult,
				"fetchVariables":       []string{"foo"},
				"requestTimeout":       requestTimeout,
			})
			assert.NoError(t, err)
			assert.Equal(t, version1ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
			assert.NotNil(t, processInstance.ProcessInstanceKey)
			assert.Equal(t, id, processInstance.BpmnProcessId)
			assert.Equal(t, int32(1), processInstance.Version)
			if withResult {
				assert.NotNil(t, processInstance.Variables)
				assert.Equal(t, "{\"foo\":\"bar\"}", processInstance.Variables)
			}

			return nil
		}
	}

	testCreateInstanceByProcessDefinitionKeyLatestVersion := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"processDefinitionKey": version2ProcessDefinitionKey,
				"variables":            variables,
				"withResult":           withResult,
				"fetchVariables":       []string{"foo"},
				"requestTimeout":       requestTimeout,
			})
			assert.NoError(t, err)
			assert.Equal(t, version2ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
			assert.NotNil(t, processInstance.ProcessInstanceKey)
			assert.Equal(t, id, processInstance.BpmnProcessId)
			assert.Equal(t, int32(2), processInstance.Version)
			if withResult {
				assert.NotNil(t, processInstance.Variables)
				assert.Equal(t, "{\"foo\":\"bar\"}", processInstance.Variables)
			}

			return nil
		}
	}

	testCheckForExistingProcessDefinitionKey := func(withResult bool, requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			_, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"processDefinitionKey": 0,
				"variables":            variables,
				"withResult":           withResult,
				"fetchVariables":       []string{"foo"},
				"requestTimeout":       requestTimeout,
			})
			assert.Error(t, err)

			return nil
		}
	}

	testSyncCreationFailsIfTimeoutExceeds := func(requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			_, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId":  id,
				"variables":      variables,
				"withResult":     true,
				"fetchVariables": []string{"foo"},
				"requestTimeout": requestTimeout,
			})
			assert.Error(t, err)

			return nil
		}
	}

	testSyncCreationReturnsAllRootScopeVariables := func(requestTimeout time.Duration) func(ctx flow.Context) error {
		return func(ctx flow.Context) error {
			client := zeebe_test.GetDaprClient(grpcPort)
			defer client.Close()

			processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId":  id,
				"variables":      variables,
				"withResult":     true,
				"fetchVariables": []string{},
				"requestTimeout": requestTimeout,
			})
			assert.NoError(t, err)
			assert.Equal(t, version2ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
			assert.NotNil(t, processInstance.ProcessInstanceKey)
			assert.Equal(t, id, processInstance.BpmnProcessId)
			assert.Equal(t, int32(2), processInstance.Version)
			assert.NotNil(t, processInstance.Variables)
			assert.Equal(t, "{\"boo\":\"baz\",\"foo\":\"bar\"}", processInstance.Variables)

			return nil
		}
	}

	flow.New(t, "Test create instance operation (async)").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers(0))).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Deploy process in version 1", deployVersion1).
		Step("Deploy process in version 2", deployVersion2).
		Step("Create instance by BPMN process ID for version 1", testCreateInstanceByBpmnProcessIdVersion1(false, 0)).
		Step("Create instance by BPMN process ID for latest version (version 2)", testCreateInstanceByBpmnProcessIdLatestVersion(false, 0)).
		Step("Check for existing BPMN process ID", testCheckForExistingBpmnProcessId(false, 0)).
		Step("Create instance by process definition key (version 1)", testCreateInstanceByProcessDefinitionKeyVersion1(false, 0)).
		Step("Create instance by process definition key (version 2)", testCreateInstanceByProcessDefinitionKeyLatestVersion(false, 0)).
		Step("Check for existing process definition key", testCheckForExistingProcessDefinitionKey(false, 0)).
		Run()

	requestTimeout := 30 * time.Second
	flow.New(t, "Test create instance operation (sync)").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/syncProcessCreation"),
			)...,
		)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers(20*time.Second))).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Deploy process in version 1", deployVersion1).
		Step("Deploy process in version 2", deployVersion2).
		Step("Create instance by BPMN process ID for version 1", testCreateInstanceByBpmnProcessIdVersion1(true, requestTimeout)).
		Step("Create instance by BPMN process ID for latest version (version 2)", testCreateInstanceByBpmnProcessIdLatestVersion(true, requestTimeout)).
		Step("Check for existing BPMN process ID", testCheckForExistingBpmnProcessId(true, requestTimeout)).
		Step("Create instance by process definition key (version 1)", testCreateInstanceByProcessDefinitionKeyVersion1(true, requestTimeout)).
		Step("Create instance by process definition key (version 2)", testCreateInstanceByProcessDefinitionKeyLatestVersion(true, requestTimeout)).
		Step("Check for existing process definition key", testCheckForExistingProcessDefinitionKey(true, requestTimeout)).
		Step("Check instance creation fails if timeout exceeds", testSyncCreationFailsIfTimeoutExceeds(10*time.Second)).
		Step("Check instance creation returns all root scope variables if fetchVariables is empty", testSyncCreationReturnsAllRootScopeVariables(requestTimeout)).
		Run()
}
