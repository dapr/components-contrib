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

package command

import (
	"context"
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
		context.Background(),
		zeebe.TestProcessFile,
		zeebe.ProcessIDModifier(id),
		zeebe.JobTypeModifier("test", jobType))
	assert.NoError(t, err)
	assert.Equal(t, id, deployment.BpmnProcessId)

	t.Run("activate a job and fetch all variables", func(t *testing.T) {
		processInstance, err := zeebe.CreateProcessInstance(cmd, context.Background(), map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables": map[string]interface{}{
				"foo": "bar",
				"bar": "foo",
			},
		})
		assert.NoError(t, err)
		time.Sleep(5 * time.Second)

		jobs, err := zeebe.ActicateJob(cmd, context.Background(), map[string]interface{}{
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
		processInstance, err := zeebe.CreateProcessInstance(cmd, context.Background(), map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables": map[string]interface{}{
				"foo": "bar",
				"bar": "foo",
			},
		})
		assert.NoError(t, err)
		time.Sleep(5 * time.Second)

		jobs, err := zeebe.ActicateJob(cmd, context.Background(), map[string]interface{}{
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
