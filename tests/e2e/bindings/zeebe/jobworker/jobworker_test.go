//go:build e2etests
// +build e2etests

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

package jobworker

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

// Initializes the calc process by deploying the calc process and by registering two workers. One for the calculation
// and one for the testing of the result
func initCalcProcess(
	cmd *command.ZeebeCommand,
	id string,
	ackWorker func(*bindings.ReadResponse) ([]byte, error),
) error {
	calcJobType := id + "-calc"
	ackJobType := id + "-ack"

	_, err := zeebe.DeployProcess(
		cmd,
		zeebe.CalcProcessFile,
		zeebe.ProcessIDModifier(id),
		zeebe.JobTypeModifier("calc", calcJobType),
		zeebe.JobTypeModifier("ack", ackJobType))
	if err != nil {
		return err
	}

	calcJob, err := zeebe.JobWorker(calcJobType)
	if err != nil {
		return err
	}

	ackJob, err := zeebe.JobWorker(ackJobType)
	if err != nil {
		return err
	}

	go calcJob.Read(zeebe.CalcWorker)
	go ackJob.Read(ackWorker)

	return nil
}

// A worker which validates if the calc result
func calcResultWorker(
	t *testing.T,
	expectedResult float64,
	count *int32,
) (chan bool, func(in *bindings.ReadResponse) ([]byte, error)) {
	ch := make(chan bool, 1)

	return ch, func(in *bindings.ReadResponse) ([]byte, error) {
		result := &zeebe.CalcResult{}
		err := json.Unmarshal(in.Data, result)
		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result.Result)

		atomic.AddInt32(count, 1)

		ch <- true

		return nil, nil
	}
}

// A worker which validates the calc variables
func calcVariablesWorker(
	t *testing.T,
	expectedOperator string,
	expectedFirstOperand float64,
	expectedSecondOperand float64,
	count *int32,
) (chan bool, func(in *bindings.ReadResponse) ([]byte, error)) {
	ch := make(chan bool, 1)

	return ch, func(in *bindings.ReadResponse) ([]byte, error) {
		vars := &zeebe.CalcVariables{}
		err := json.Unmarshal(in.Data, vars)
		assert.NoError(t, err)
		assert.Equal(t, expectedOperator, vars.Operator)
		assert.Equal(t, expectedFirstOperand, vars.FirstOperand)
		assert.Equal(t, expectedSecondOperand, vars.SecondOperand)
		atomic.AddInt32(count, 1)

		ch <- true

		return nil, nil
	}
}

// A worker which validates the retry logic of the job worker
func retryWorker(count *int32) (chan bool, func(in *bindings.ReadResponse) ([]byte, error)) {
	ch := make(chan bool, 1)

	return ch, func(in *bindings.ReadResponse) ([]byte, error) {
		atomic.AddInt32(count, 1)

		if in.Metadata["X-Zeebe-Retries"] == "1" {
			ch <- true
		}

		return nil, errors.New("some failure")
	}
}

// A worker which checks if all headers will bereturned from the job worker
func headerWorker(t *testing.T, id string, count *int32) (chan bool, func(in *bindings.ReadResponse) ([]byte, error)) {
	ch := make(chan bool, 1)

	return ch, func(in *bindings.ReadResponse) ([]byte, error) {
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
		assert.Contains(t, in.Metadata, "process-header-1")
		assert.Contains(t, in.Metadata, "process-header-2")

		assert.Equal(t, id+"-test", in.Metadata["X-Zeebe-Job-Type"])
		assert.Equal(t, id, in.Metadata["X-Zeebe-Bpmn-Process-Id"])
		assert.Equal(t, "1", in.Metadata["X-Zeebe-Process-Definition-Version"])
		assert.Equal(t, "Activity_test", in.Metadata["X-Zeebe-Element-Id"])
		assert.Equal(t, "test", in.Metadata["X-Zeebe-Worker"])
		assert.Equal(t, "3", in.Metadata["X-Zeebe-Retries"])
		assert.Equal(t, "1", in.Metadata["process-header-1"])
		assert.Equal(t, "2", in.Metadata["process-header-2"])

		atomic.AddInt32(count, 1)

		ch <- true

		return nil, nil
	}
}

func TestJobworker(t *testing.T) {
	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	t.Run("execute calc process by creating an instance", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := calcResultWorker(t, float64(12), &count)

		err = initCalcProcess(cmd, id, ackWorker)
		assert.NoError(t, err)

		_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		zeebe.InterruptProcess()
	})

	t.Run("execute calc process by sending a message", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := calcResultWorker(t, float64(4), &count)

		err = initCalcProcess(cmd, id, ackWorker)
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

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, _ := cmd.Invoke(req)
		assert.NotNil(t, res)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		zeebe.InterruptProcess()
	})

	t.Run("retry a process three times", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := retryWorker(&count)

		err = zeebe.InitTestProcess(cmd, id, ackWorker)
		assert.NoError(t, err)

		_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(3), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		zeebe.InterruptProcess()
	})

	t.Run("contains all headers", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := headerWorker(t, id, &count)
		err = zeebe.InitTestProcess(cmd, id, ackWorker, zeebe.MetadataPair{Key: "workerName", Value: "test"})
		assert.NoError(t, err)

		_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"bpmnProcessId": id,
		})
		assert.NoError(t, err)

		select {
		case <-ch:
			assert.Equal(t, int32(1), atomic.LoadInt32(&count))
		case <-time.After(5 * time.Second):
			assert.FailNow(t, "read timeout")
		}

		zeebe.InterruptProcess()
	})

	t.Run("fetch all variables", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := calcVariablesWorker(t, "/", float64(12), float64(3), &count)
		err = zeebe.InitTestProcess(cmd, id, ackWorker)
		assert.NoError(t, err)

		_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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

		zeebe.InterruptProcess()
	})

	t.Run("fetch selected variables", func(t *testing.T) {
		var count int32
		id := zeebe.TestID()
		ch, ackWorker := calcVariablesWorker(t, "", float64(12), float64(3), &count)
		err = zeebe.InitTestProcess(cmd, id, ackWorker, zeebe.MetadataPair{
			Key:   "fetchVariables",
			Value: "firstOperand,secondOperand",
		})
		assert.NoError(t, err)

		_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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

		zeebe.InterruptProcess()
	})
}
