// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"encoding/json"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestSetVariables(t *testing.T) {
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
		"variables": map[string]interface{}{
			"foo": "bar",
		},
	})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, processInstance.ProcessInstanceKey)

	t.Run("set variables for an instance", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"elementInstanceKey": processInstance.ProcessInstanceKey,
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.SetVariablesOperation}
		res, err := cmd.Invoke(req)
		assert.NoError(t, err)

		variableResponse := &pb.SetVariablesResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})

	t.Run("return error for not existing element instance", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"elementInstanceKey": 0,
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.SetVariablesOperation}
		_, err = cmd.Invoke(req)
		assert.Error(t, err)
	})
}
