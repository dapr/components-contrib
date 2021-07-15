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

func TestDeployProcess(t *testing.T) {
	id := zeebe.TestID()
	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	t.Run("deploy a new process", func(t *testing.T) {
		deployment, err := zeebe.DeployProcess(cmd, zeebe.TestProcessFile, zeebe.ProcessIDModifier(id))
		assert.NoError(t, err)
		assert.Equal(t, id, deployment.BpmnProcessId)
		assert.Equal(t, int32(1), deployment.Version)
		assert.NotNil(t, deployment.ProcessDefinitionKey)
		assert.Equal(t, zeebe.TestProcessFile, deployment.ResourceName)
	})

	t.Run("override an existing process with another version", func(t *testing.T) {
		deployment, err := zeebe.DeployProcess(
			// changing the name results in a new version
			cmd, zeebe.TestProcessFile, zeebe.ProcessIDModifier(id), zeebe.NameModifier(id))
		assert.NoError(t, err)
		assert.Equal(t, id, deployment.BpmnProcessId)
		assert.Equal(t, int32(2), deployment.Version)
		assert.NotNil(t, deployment.ProcessDefinitionKey)
		assert.Equal(t, zeebe.TestProcessFile, deployment.ResourceName)
	})
}
