//go:build e2etests
// +build e2etests

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

package command

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestUpdateJobRetries(t *testing.T) {
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

	_, err = zeebe.CreateProcessInstance(cmd, context.Background(), map[string]interface{}{
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

	t.Run("update job retries", func(t *testing.T) {
		data, err := json.Marshal(map[string]interface{}{
			"jobKey":  job.Key,
			"retries": 3,
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.UpdateJobRetriesOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)
		assert.Nil(t, res.Data)
		assert.Nil(t, res.Metadata)
	})

	t.Run("should fail for not existing job", func(t *testing.T) {
		data, err := json.Marshal(map[string]interface{}{
			"jobKey":  1,
			"retries": 3,
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.UpdateJobRetriesOperation}
		_, err = cmd.Invoke(context.Background(), req)
		assert.Error(t, err)
	})
}
