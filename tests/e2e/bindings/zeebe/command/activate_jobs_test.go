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
	"encoding/json"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/entities"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestActivateJobs(t *testing.T) {
	t.Parallel()

	id := zeebe.TestID()
	jobType := id + "-test"

	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	deployment, err := zeebe.DeployProcess(
		cmd,
		zeebe.TestProcessFile,
		zeebe.ProcessIDModifier(id),
		zeebe.JobTypeModifier("test", jobType))
	assert.NoError(t, err)
	assert.Equal(t, id, deployment.BpmnProcessId)

	t.Run("activate a job", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"jobType":           jobType,
			"maxJobsToActivate": 100,
			"timeout":           "10m",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.ActivateJobsOperation}
		res, err := cmd.Invoke(req)
		assert.NoError(t, err)
		assert.NotNil(t, res)

		jobs := &[]entities.Job{}
		err = json.Unmarshal(res.Data, jobs)
		assert.NoError(t, err)
		// There is currently an issue which prevents the command to return the jobs: https://github.com/camunda-cloud/zeebe/issues/5925
		// assert.Equal(t, 1, len(*jobs))
		assert.Nil(t, res.Metadata)
	})
}
