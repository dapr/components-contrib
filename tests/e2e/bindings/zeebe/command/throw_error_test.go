// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestThrowError(t *testing.T) {
	t.Parallel()

	id := zeebe.TestID()
	jobType := id + "-test"
	workerName := "test"

	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	deployment, err := zeebe.DeployProcess(
		cmd,
		zeebe.TestProcessFile,
		zeebe.ProcessIDModifier(id),
		zeebe.JobTypeModifier("test", jobType))
	assert.NoError(t, err)
	assert.Equal(t, id, deployment.BpmnProcessId)

	_, err = zeebe.CreateProcessInstance(cmd, map[string]interface{}{
		"bpmnProcessId": id,
		"version":       1,
		"variables": map[string]interface{}{
			"foo": "bar",
			"bar": "foo",
		},
	})
	assert.NoError(t, err)
	time.Sleep(5 * time.Second)

	jobs, err := zeebe.ActicateJob(cmd, map[string]interface{}{
		"jobType":           jobType,
		"maxJobsToActivate": 100,
		"timeout":           "10m",
		"workerName":        workerName,
	})

	assert.NoError(t, err)
	assert.NotNil(t, jobs)
	assert.Equal(t, 1, len(*jobs))

	job := (*jobs)[0]
	assert.NotNil(t, job.Key)

	t.Run("throw an error", func(t *testing.T) {
		data, err := json.Marshal(map[string]interface{}{
			"jobKey":       job.Key,
			"errorCode":    "test-error",
			"errorMessage": "test error",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.ThrowErrorOperation}
		res, err := cmd.Invoke(req)
		assert.NoError(t, err)
		assert.Nil(t, res.Data)
		assert.Nil(t, res.Metadata)
	})

	t.Run("should fail for not existing job", func(t *testing.T) {
		data, err := json.Marshal(map[string]interface{}{
			"jobKey":       1,
			"errorCode":    "test-error",
			"errorMessage": "test error",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.ThrowErrorOperation}
		_, err = cmd.Invoke(req)
		assert.Error(t, err)
	})
}
