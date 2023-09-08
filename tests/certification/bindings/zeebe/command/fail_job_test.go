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

package command_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
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

func deployProcess(t *testing.T, id string, grpcPort int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		deployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id),
			zeebe_test.RetryModifier("zeebe-jobworker-test", 3))

		assert.NoError(t, err)
		assert.Equal(t, id, deployment.Deployments[0].Metadata.Process.BpmnProcessId)

		return nil
	}
}

func createProcessInstance(t *testing.T, id string, grpcPort int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		_, err := zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables": map[string]interface{}{
				"foo": "bar",
				"bar": "foo",
			},
		})
		assert.NoError(t, err)

		return nil
	}
}

func TestFailJobOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	id := zeebe_test.TestID()
	workerName := "test"

	var activatedJobKey int64

	activateJob := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

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
		activatedJobKey = job.Key

		return nil
	}

	failJob := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"jobKey":       activatedJobKey,
			"retries":      3,
			"errorMessage": "test error",
		})
		assert.NoError(t, err)

		res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.FailJobOperation, data, nil)
		assert.NoError(t, err)
		assert.Nil(t, res.Data)
		assert.Nil(t, res.Metadata)

		return nil
	}

	failForNotExistingJob := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		data, err := json.Marshal(map[string]interface{}{
			"jobKey":       1,
			"retries":      3,
			"errorMessage": "test error",
		})
		assert.NoError(t, err)

		_, err = zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.FailJobOperation, data, nil)
		assert.Error(t, err)

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
		Step("Deploy process", deployProcess(t, id, grpcPort)).
		Step("Create process instance", createProcessInstance(t, id, grpcPort)).
		Step("Activate the job", activateJob).
		Step("Fail the job", failJob).
		Step("Should fail for not existing job", failForNotExistingJob).
		Run()
}

func TestFailJobOperationWithRetryBackOff(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) ([]byte, error) {
				atomic.AddInt32(&count, 1)

				retries, err := strconv.Atoi(in.Metadata["X-Zeebe-Retries"])
				assert.NoError(t, err)

				jobKey, err := strconv.ParseInt(in.Metadata["X-Zeebe-Job-Key"], 10, 64)
				assert.NoError(t, err)

				if retries == 1 {
					ch <- true

					return []byte("{}"), nil
				}

				client := zeebe_test.GetDaprClient(grpcPort)
				defer client.Close()

				data, err := json.Marshal(map[string]interface{}{
					"jobKey":       jobKey,
					"retries":      retries - 1,
					"errorMessage": "test error",
					"retryBackOff": 15 * time.Second,
				})
				assert.NoError(t, err)

				res, err := zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.FailJobOperation, data, nil)
				assert.NoError(t, err)
				assert.Nil(t, res.Data)
				assert.Nil(t, res.Metadata)

				return nil, errors.New("some failure")
			}),
		)
	}

	checkRetryBackOfWasApplied := func(ctx flow.Context) error {
		start := time.Now()

		select {
		case <-ch:
			elapsed := time.Since(start)
			assert.True(t, elapsed > 30) // 3 retries and a retryBackOff of 10s
			assert.Equal(t, int32(3), atomic.LoadInt32(&count))
		case <-time.After(60 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test fail job operation with retry back-off").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/retryBackOff"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Deploy process", deployProcess(t, id, grpcPort)).
		Step("Create process instance", createProcessInstance(t, id, grpcPort)).
		Step("Check if retry back-off was applied", checkRetryBackOfWasApplied).
		Run()
}
