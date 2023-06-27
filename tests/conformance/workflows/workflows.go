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

package workflows

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/workflows"
)

var testLogger = logger.NewLogger("workflowsTest")

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(component string, operations []string, conf map[string]interface{}) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "workflows",
			ComponentName: component,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	return tc
}

// ConformanceTests runs conf tests for workflows.
func ConformanceTests(t *testing.T, props map[string]string, workflowItem workflows.Workflow, config TestConfig) {
	// Test vars
	t.Run("init", func(t *testing.T) {
		err := workflowItem.Init(workflows.Metadata{Base: metadata.Base{
			Properties: props,
		}})
		assert.NoError(t, err)
	})

	// Everything is within the same task since the workflow needs to persist between operations
	t.Run("start", func(t *testing.T) {
		testLogger.Info("Start test running...")

		inputBytes, _ := json.Marshal(10) // Time that the activity within the workflow runs for

		testInstanceID := "TestID"
		t.Run("start", func(t *testing.T) {
			req := &workflows.StartRequest{
				InstanceID:    testInstanceID,
				WorkflowName:  "TestWorkflow",
				WorkflowInput: inputBytes,
				Options: map[string]string{
					"task_queue": "TestTaskQueue",
				},
			}

			startRes, err := workflowItem.Start(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, testInstanceID, startRes.InstanceID)
		})

		t.Run("get after start", func(t *testing.T) {
			resp, err := workflowItem.Get(context.Background(), &workflows.GetRequest{InstanceID: testInstanceID})
			require.NoError(t, err)
			assert.Equal(t, "TestID", resp.Workflow.InstanceID)
			assert.Equal(t, "Running", resp.Workflow.RuntimeStatus)
		})

		// Let the workflow run for a bit and make sure it doesn't complete on its own
		time.Sleep(5 * time.Second)

		t.Run("get after wait", func(t *testing.T) {
			resp, err := workflowItem.Get(context.Background(), &workflows.GetRequest{InstanceID: testInstanceID})
			require.NoError(t, err)
			assert.Equal(t, "Running", resp.Workflow.RuntimeStatus)
		})

		t.Run("terminate", func(t *testing.T) {
			err := workflowItem.Terminate(context.Background(), &workflows.TerminateRequest{InstanceID: testInstanceID})
			assert.NoError(t, err)
		})

		// Give the workflow time to process the terminate request
		time.Sleep(5 * time.Second)

		t.Run("get after terminate", func(t *testing.T) {
			resp, err := workflowItem.Get(context.Background(), &workflows.GetRequest{InstanceID: testInstanceID})
			require.NoError(t, err)
			assert.Equal(t, "Terminated", resp.Workflow.RuntimeStatus)
			assert.Equal(t, "TestID", resp.Workflow.InstanceID)
		})
		testLogger.Info("Start test done.")
	})
}
