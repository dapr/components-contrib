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

package jobworker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	bindings_zeebe_jobworker "github.com/dapr/components-contrib/bindings/zeebe/jobworker"
	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/config/protocol"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	dapr_client "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
)

type calcVariables struct {
	Operator      string  `json:"operator"`
	FirstOperand  float64 `json:"firstOperand"`
	SecondOperand float64 `json:"secondOperand"`
}

type calcResult struct {
	Result float64 `json:"result"`
}

// Deploy the calc process
func deployCalcProcess(
	client dapr_client.Client,
	id string,
) error {
	_, err := zeebe_test.DeployResource(
		client,
		context.Background(),
		zeebe_test.CalcProcessFile,
		1,
		zeebe_test.IDModifier(id))

	if err != nil {
		return err
	}

	return nil
}

// Deploy the test process
func deployTestProcess(
	client dapr_client.Client,
	id string,
	retries int,
) error {
	_, err := zeebe_test.DeployResource(
		client,
		context.Background(),
		zeebe_test.TestProcessFile,
		1,
		zeebe_test.IDModifier(id),
		zeebe_test.RetryModifier("zeebe-jobworker-test", retries))

	if err != nil {
		return err
	}

	return nil
}

// CalcProcessWorker is a simple calculation worker that can be autocompleted or not.
//
// The worker will return the result of the calculation in the "result" property of the response.
// All the properties that will be returned from a job worker will be stored as process variables.
// Thus, the process will contain a "result" process variable with the result of the calculation
// if the worker was successful processes.
//
// Uses the "zeebe-jobworker-calc-exec" job
func calcProcessWorker(
	t *testing.T,
	expectedAutocomplete bool,
	grpcPort int,
	ctx flow.Context,
) func(context context.Context, in *common.BindingEvent) ([]byte, error) {
	return func(c context.Context, in *common.BindingEvent) ([]byte, error) {
		variables := &calcVariables{}
		err := json.Unmarshal(in.Data, variables)
		if err != nil {
			return nil, err
		}

		result := calcResult{}
		switch variables.Operator {
		case "+":
			result.Result = variables.FirstOperand + variables.SecondOperand
		case "-":
			result.Result = variables.FirstOperand - variables.SecondOperand
		case "/":
			result.Result = variables.FirstOperand / variables.SecondOperand
		case "*":
			result.Result = variables.FirstOperand * variables.SecondOperand
		default:
			return nil, fmt.Errorf("unexpected operator: %s", variables.Operator)
		}

		response, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}

		autocomplete, err := strconv.ParseBool(in.Metadata["X-Zeebe-Autocomplete"])
		if err != nil {
			return nil, err
		}

		assert.Equal(t, expectedAutocomplete, autocomplete)

		if autocomplete {
			ctx.Log("======== Autocomplete calcProcessWorker")
			return response, nil
		}

		ctx.Log("======== Manually complete calcProcessWorker")

		jobKey, err := strconv.ParseInt(in.Metadata["X-Zeebe-Job-Key"], 10, 64)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(map[string]interface{}{
			"jobKey":    jobKey,
			"variables": result,
		})
		if err != nil {
			return nil, err
		}

		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		_, err = zeebe_test.ExecCommandOperation(ctx, client, bindings_zeebe_command.CompleteJobOperation, data, nil)
		if err != nil {
			return nil, err
		}

		return []byte("{}"), nil
	}
}

// A worker which validates the calc result
//
// This worker will check the "result" process variable which was created by the "calcWorker" and compares the
// value of the variable with the with the expected result.
//
// The chanel is used to notify that the worker was processed, because we instantiate the process in an async way and we need
// to execute adittional test logic after the process was finished.
//
// The counter is used to count how many times the worker was called.
//
// Uses the "zeebe-jobworker-calc-ack" job
func calcResultWorker(
	t *testing.T,
	expectedResult float64,
	ch chan bool,
	count *int32,
) func(context context.Context, in *common.BindingEvent) ([]byte, error) {
	return func(context context.Context, in *common.BindingEvent) ([]byte, error) {
		result := &calcResult{}
		err := json.Unmarshal(in.Data, result)
		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result.Result)

		atomic.AddInt32(count, 1)

		ch <- true

		return []byte("{}"), nil
	}
}

// A worker which validates the calc variables. It gets the variables which were saved during instance creation
// and checks egainst the given values.
//
// Uses the "zeebe-jobworker-test" job
func calcVariablesWorker(
	t *testing.T,
	expectedOperator string,
	expectedFirstOperand float64,
	expectedSecondOperand float64,
	ch chan bool,
	count *int32,
) func(context context.Context, in *common.BindingEvent) ([]byte, error) {
	return func(context context.Context, in *common.BindingEvent) ([]byte, error) {
		vars := &calcVariables{}
		err := json.Unmarshal(in.Data, vars)
		assert.NoError(t, err)
		assert.Equal(t, expectedOperator, vars.Operator)
		assert.Equal(t, expectedFirstOperand, vars.FirstOperand)
		assert.Equal(t, expectedSecondOperand, vars.SecondOperand)
		atomic.AddInt32(count, 1)

		ch <- true

		return []byte("{}"), nil
	}
}

func TestCalcJobworkerByInstanceCreationWithAutocompleteTrue(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerCalcExecName, calcProcessWorker(t, true, grpcPort, ctx)),
			s.AddBindingInvocationHandler(zeebe_test.JobworkerCalcAckName, calcResultWorker(t, 12, ch, &count)),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployCalcProcess(client, id)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"variables": map[string]interface{}{
				"operator":      "*",
				"firstOperand":  3,
				"secondOperand": 4,
			},
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(50 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test calc process by creating an instance with auto-completion enabled").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestCalcJobworkerBySendingMessageWithAutocompleteFalse(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerCalcExecName, calcProcessWorker(t, false, grpcPort, ctx)),
			s.AddBindingInvocationHandler(zeebe_test.JobworkerCalcAckName, calcResultWorker(t, 4, ch, &count)),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployCalcProcess(client, id)
		assert.NoError(t, err)

		data, _ := json.Marshal(map[string]interface{}{
			"messageName": "start-calc",
			"timeToLive":  "5m",
			"variables": map[string]interface{}{
				"operator":      "/",
				"firstOperand":  12,
				"secondOperand": 3,
			},
		})
		assert.NotNil(t, data)

		_, err = zeebe_test.ExecCommandOperation(context.Background(), client, bindings_zeebe_command.PublishMessageOperation, data, nil)
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test calc process by sending a message with auto-completion disabled").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("app", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/autocompleteDisabled"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestWorkerRetriesThreeTimes(t *testing.T) {
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

				if in.Metadata["X-Zeebe-Retries"] == "1" {
					ch <- true

					return []byte("{}"), nil
				}

				return nil, errors.New("some failure")
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 3)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(3), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker retries three times").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestWorkerReceivesAllHeaders(t *testing.T) {
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
				assert.Contains(t, in.Metadata, "X-Zeebe-Job-Key")
				assert.Contains(t, in.Metadata, "X-Zeebe-Job-Type")
				assert.Contains(t, in.Metadata, "X-Zeebe-Process-Instance-Key")
				assert.Contains(t, in.Metadata, "X-Zeebe-Bpmn-Process-Id")
				assert.Contains(t, in.Metadata, "X-Zeebe-Process-Definition-Version")
				assert.Contains(t, in.Metadata, "X-Zeebe-Process-Definition-Key")
				assert.Contains(t, in.Metadata, "X-Zeebe-Element-Id")
				assert.Contains(t, in.Metadata, "X-Zeebe-Element-Instance-Key")
				assert.Contains(t, in.Metadata, "X-Zeebe-Worker")
				assert.Contains(t, in.Metadata, "X-Zeebe-Retries")
				assert.Contains(t, in.Metadata, "X-Zeebe-Deadline")
				assert.Contains(t, in.Metadata, "X-Zeebe-Autocomplete")
				assert.Contains(t, in.Metadata, "Process-Header-1")
				assert.Contains(t, in.Metadata, "Process-Header-2")

				assert.Equal(t, zeebe_test.JobworkerTestName, in.Metadata["X-Zeebe-Job-Type"])
				assert.Equal(t, id, in.Metadata["X-Zeebe-Bpmn-Process-Id"])
				assert.Equal(t, "1", in.Metadata["X-Zeebe-Process-Definition-Version"])
				assert.Equal(t, "Activity_test", in.Metadata["X-Zeebe-Element-Id"])
				assert.Equal(t, "test-worker", in.Metadata["X-Zeebe-Worker"])
				assert.Equal(t, "3", in.Metadata["X-Zeebe-Retries"])
				assert.Equal(t, "true", in.Metadata["X-Zeebe-Autocomplete"])
				assert.Equal(t, "1", in.Metadata["Process-Header-1"])
				assert.Equal(t, "2", in.Metadata["Process-Header-2"])

				atomic.AddInt32(&count, 1)

				ch <- true

				return []byte("{}"), nil
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 3)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker receives all headers").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestWorkerFetchesAllVariables(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, calcVariablesWorker(t, "/", float64(12), float64(3), ch, &count)),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 1)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"variables": map[string]interface{}{
				"operator":      "/",
				"firstOperand":  12,
				"secondOperand": 3,
			},
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker fetches all variables").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestWorkerFetchesSelectedVariables(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, calcVariablesWorker(t, "", float64(12), float64(3), ch, &count)),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 1)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
			"variables": map[string]interface{}{
				"operator":      "/",
				"firstOperand":  12,
				"secondOperand": 3,
			},
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker fetches selected variables").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/fetchVariables"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestJobWorkerHandlesOutage(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		sim := simulate.PeriodicError(ctx, 100)
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {
				if err := sim(); err != nil {
					return nil, err
				}

				atomic.AddInt32(&count, 1)
				ch <- true

				return in.Data, nil
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 100)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(150 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker handles outage").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/fetchVariables"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Interrupt network port for Zeebe gateway", network.InterruptNetwork(30*time.Second, nil, nil, "26500")).
		Step("wait", flow.Sleep(30*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestWorkerHandlesConcurrency(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(7)
	grpcPort1 := ports[0]
	httpPort1 := ports[1]
	appPort1 := ports[2]
	grpcPort2 := ports[3]
	httpPort2 := ports[4]
	appPort2 := ports[5]
	profilePort := ports[6]

	id := zeebe_test.TestID()
	var count1 int32
	var count2 int32

	workers1 := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {
				atomic.AddInt32(&count1, 1)

				return in.Data, nil
			}),
		)
	}

	workers2 := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {
				atomic.AddInt32(&count2, 1)

				return in.Data, nil
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort1)
		defer client.Close()

		err := deployTestProcess(client, id, 1)
		assert.NoError(t, err)

		for i := 0; i < 50; i++ {
			_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId": id,
			})
			assert.NoError(t, err)
		}

		return nil
	}

	checkCounts := func(ctx flow.Context) error {
		assert.NotEqual(t, "0", count1)
		assert.NotEqual(t, "0", count2)

		assert.Equal(t, int32(50), count1+count2)

		return nil
	}

	flow.New(t, "Test worker handles concurrency").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp1", fmt.Sprintf(":%d", appPort1), workers1)).
		Step(sidecar.Run(zeebe_test.SidecarName+"1",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort1)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort1)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort1)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step(app.Run("workerApp2", fmt.Sprintf(":%d", appPort2), workers2)).
		Step(sidecar.Run(zeebe_test.SidecarName+"2",
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort2)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort2)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort2)),
				embedded.WithProfilePort(strconv.Itoa(profilePort)),
				embedded.WithResourcesPath("components/standard"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(10*time.Second)).
		Step("Check counts", checkCounts).
		Run()
}

func TestJobWorkerHandlesTls(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool)
	var count int32

	workers := func(ctx flow.Context, s common.Service) error {
		return multierr.Combine(
			s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, in *common.BindingEvent) (out []byte, err error) {
				atomic.AddInt32(&count, 1)
				ch <- true

				return in.Data, nil
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 100)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		close(ch)

		return nil
	}

	flow.New(t, "Test worker handles TLS").
		Step("Provide key and cert", zeebe_test.ProvideKeyAndCert("../certs")).
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeTlsYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath("components/tlsEnabled"),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func TestJobWorkerHandlesRetryBackOff(t *testing.T) {
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

				if in.Metadata["X-Zeebe-Retries"] == "1" {
					ch <- true

					return []byte("{}"), nil
				}

				return nil, errors.New("some failure")
			}),
		)
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 3)
		assert.NoError(t, err)

		_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

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

	flow.New(t, "Test worker handles retry back-off").
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
		Step("Execute process", executeProcess).
		Step("Waiting for the job to complete", flow.Sleep(5*time.Second)).
		Run()
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	bindingsRegistry := bindings_loader.NewRegistry()
	bindingsRegistry.Logger = log
	bindingsRegistry.RegisterInputBinding(func(l logger.Logger) bindings.InputBinding {
		return bindings_zeebe_jobworker.NewZeebeJobWorker(l)
	}, "zeebe.jobworker")
	bindingsRegistry.RegisterOutputBinding(func(l logger.Logger) bindings.OutputBinding {
		return bindings_zeebe_command.NewZeebeCommand(l)
	}, "zeebe.command")

	return []embedded.Option{
		embedded.WithBindings(bindingsRegistry),
	}
}
