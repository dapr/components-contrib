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
	"encoding/json"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestCancelInstance(t *testing.T) {
	t.Parallel()

	id := zeebe.TestID()
	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	// Deploy process
	deployment, err := zeebe.DeployProcess(cmd, context.Background(), zeebe.TestProcessFile, zeebe.ProcessIDModifier(id))
	assert.NoError(t, err)
	assert.Equal(t, id, deployment.BpmnProcessId)

	// Create instance
	processInstance, err := zeebe.CreateProcessInstance(cmd, context.Background(), map[string]interface{}{
		"bpmnProcessId": id,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, processInstance.ProcessInstanceKey)

	t.Run("cancel an instance", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"processInstanceKey": processInstance.ProcessInstanceKey,
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.CancelInstanceOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)
		assert.Nil(t, res.Data)
		assert.Nil(t, res.Metadata)
	})

	t.Run("return error for not existing instance", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"processInstanceKey": 0,
		})
		assert.NotNil(t, data)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.CancelInstanceOperation}
		_, err = cmd.Invoke(context.Background(), req)
		assert.Error(t, err)
	})
}
