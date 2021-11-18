// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"testing"
	"time"

	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestActivateJobs(t *testing.T) {
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

	t.Run("activate a job and fetch all variables", func(t *testing.T) {
		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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
		assert.Equal(t, jobType, job.Type)
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
	})

	t.Run("activate a job and fetch only the foo variable", func(t *testing.T) {
		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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
			"fetchVariables":    [1]string{"foo"},
		})

		assert.NoError(t, err)
		assert.NotNil(t, jobs)
		assert.Equal(t, 1, len(*jobs))

		job := (*jobs)[0]
		assert.NotNil(t, job.Key)
		assert.Equal(t, jobType, job.Type)
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
	})
}
