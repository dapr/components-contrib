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
	"strconv"
	"testing"
	"time"

	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
)

func TestActivateJobsOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	id := zeebe_test.TestID()
	workerName := "test"

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

	activateJobAndFetchAllVars := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables": map[string]interface{}{
				"foo": "bar",
				"bar": "foo",
			},
		})
		assert.NoError(t, err)

		jobs, err := zeebe_test.ActicateJob(client, ctx, map[string]interface{}{
			"jobType":           zeebe_test.JobworkerTestName,
			"maxJobsToActivate": 100,
			"timeout":           "10m",
			"workerName":        workerName,
		})

		assert.NoError(t, err)
		assert.NotNil(t, jobs)
		assert.Equal(t, 1, len(*jobs))

		job := (*jobs)[0]
		assert.NotNil(t, job.Key)
		assert.Equal(t, zeebe_test.JobworkerTestName, job.Type)
		assert.Equal(t, processInstance.ProcessInstanceKey, job.ProcessInstanceKey)
		assert.Equal(t, processInstance.BpmnProcessId, job.BpmnProcessId)
		assert.Equal(t, processInstance.ProcessDefinitionKey, job.ProcessDefinitionKey)
		assert.NotNil(t, job.ElementInstanceKey)
		assert.Equal(t, "Activity_test", job.ElementId)
		assert.Equal(t, "{\"process-header-1\":\"1\",\"process-header-2\":\"2\"}", job.CustomHeaders)
		assert.Equal(t, workerName, job.Worker)
		assert.Equal(t, int32(1), job.Retries)
		assert.NotNil(t, job.Deadline)
		assert.Equal(t, "{\"bar\":\"foo\",\"foo\":\"bar\"}", job.Variables)

		return nil
	}

	activateJobAndFetchOnlyFooVar := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		processInstance, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables": map[string]interface{}{
				"foo": "bar",
				"bar": "foo",
			},
		})
		assert.NoError(t, err)

		jobs, err := zeebe_test.ActicateJob(client, ctx, map[string]interface{}{
			"jobType":           zeebe_test.JobworkerTestName,
			"maxJobsToActivate": 100,
			"timeout":           "10m",
			"workerName":        workerName,
			"fetchVariables":    [1]string{"foo"},
			"requestTimeout":    30 * time.Second,
		})

		assert.NoError(t, err)
		assert.NotNil(t, jobs)
		assert.Equal(t, 1, len(*jobs))

		job := (*jobs)[0]
		assert.NotNil(t, job.Key)
		assert.Equal(t, zeebe_test.JobworkerTestName, job.Type)
		assert.Equal(t, processInstance.ProcessInstanceKey, job.ProcessInstanceKey)
		assert.Equal(t, processInstance.BpmnProcessId, job.BpmnProcessId)
		assert.Equal(t, processInstance.ProcessDefinitionKey, job.ProcessDefinitionKey)
		assert.NotNil(t, job.ElementInstanceKey)
		assert.Equal(t, "Activity_test", job.ElementId)
		assert.Equal(t, "{\"process-header-1\":\"1\",\"process-header-2\":\"2\"}", job.CustomHeaders)
		assert.Equal(t, workerName, job.Worker)
		assert.Equal(t, int32(1), job.Retries)
		assert.NotNil(t, job.Deadline)
		assert.Equal(t, "{\"foo\":\"bar\"}", job.Variables)

		return nil
	}

	flow.New(t, "Test activate job operation").
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
		Step("Activate a job and fetch all variables", activateJobAndFetchAllVars).
		Step("Activate a job and fetch only the foo variable", activateJobAndFetchOnlyFooVar).
		Run()
}
