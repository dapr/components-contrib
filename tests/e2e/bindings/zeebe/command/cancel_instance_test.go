// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
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
	deployment, err := zeebe.DeployProcess(cmd, zeebe.TestProcessFile, zeebe.ProcessIDModifier(id))
	assert.NoError(t, err)
	assert.Equal(t, id, deployment.BpmnProcessId)

	// Create instance
	processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
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
		res, err := cmd.Invoke(req)
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
		_, err = cmd.Invoke(req)
		assert.Error(t, err)
	})
}
