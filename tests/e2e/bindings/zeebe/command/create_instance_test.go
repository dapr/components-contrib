// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"testing"

	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestCreateInstance(t *testing.T) {
	t.Parallel()

	id := zeebe.TestID()
	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	variables := map[string]interface{}{
		"foo": "bar",
	}

	// Deploy version 1
	firstDeployment, err := zeebe.DeployProcess(cmd, zeebe.TestProcessFile, zeebe.ProcessIDModifier(id))
	assert.NoError(t, err)
	assert.Equal(t, id, firstDeployment.BpmnProcessId)
	assert.Equal(t, int32(1), firstDeployment.Version)

	// Deploy version 2
	secondDeployment, err := zeebe.DeployProcess(
		// changing the name results in a new version
		cmd, zeebe.TestProcessFile, zeebe.ProcessIDModifier(id), zeebe.NameModifier(id))
	assert.NoError(t, err)
	assert.Equal(t, id, secondDeployment.BpmnProcessId)
	assert.Equal(t, int32(2), secondDeployment.Version)

	t.Run("create instance by BPMN process ID for version 1", func(t *testing.T) {
		t.Parallel()

		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"bpmnProcessId": id,
			"version":       1,
			"variables":     variables,
		})
		assert.NoError(t, err)
		assert.Equal(t, firstDeployment.ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
		assert.NotNil(t, processInstance.ProcessInstanceKey)
		assert.Equal(t, id, processInstance.BpmnProcessId)
		assert.Equal(t, int32(1), processInstance.Version)
	})

	t.Run("create instance by BPMN process ID for latest version (version 2)", func(t *testing.T) {
		t.Parallel()

		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"bpmnProcessId": id,
			"variables":     variables,
		})
		assert.NoError(t, err)
		assert.Equal(t, secondDeployment.ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
		assert.NotNil(t, processInstance.ProcessInstanceKey)
		assert.Equal(t, id, processInstance.BpmnProcessId)
		assert.Equal(t, int32(2), processInstance.Version)
	})

	t.Run("return error for not existing BPMN process ID", func(t *testing.T) {
		t.Parallel()

		_, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"bpmnProcessId": "not-existing",
			"variables":     variables,
		})
		assert.Error(t, err)
	})

	t.Run("create instance by process definition key (version 1)", func(t *testing.T) {
		t.Parallel()

		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"processDefinitionKey": firstDeployment.ProcessDefinitionKey,
			"variables":            variables,
		})
		assert.NoError(t, err)
		assert.Equal(t, firstDeployment.ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
		assert.NotNil(t, processInstance.ProcessInstanceKey)
		assert.Equal(t, id, processInstance.BpmnProcessId)
		assert.Equal(t, int32(1), processInstance.Version)
	})

	t.Run("create instance by process definition key (version 2)", func(t *testing.T) {
		t.Parallel()

		processInstance, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"processDefinitionKey": secondDeployment.ProcessDefinitionKey,
			"variables":            variables,
		})
		assert.NoError(t, err)
		assert.Equal(t, secondDeployment.ProcessDefinitionKey, processInstance.ProcessDefinitionKey)
		assert.NotNil(t, processInstance.ProcessInstanceKey)
		assert.Equal(t, id, processInstance.BpmnProcessId)
		assert.Equal(t, int32(2), processInstance.Version)
	})

	t.Run("return error for not existing process definition key", func(t *testing.T) {
		t.Parallel()

		_, err := zeebe.CreateProcessInstance(cmd, map[string]interface{}{
			"processDefinitionKey": 0,
			"variables":            variables,
		})
		assert.Error(t, err)
	})
}
