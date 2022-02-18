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
