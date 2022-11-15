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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/workflows"
)

var testLogger = logger.NewLogger("workflowsTest")

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(component string, allOperations bool, operations []string, conf map[string]interface{}) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "workflows",
			ComponentName: component,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
	}

	return tc
}

// ConformanceTests runs conf tests for workflows.
func ConformanceTests(t *testing.T, props map[string]string, workflowItem workflows.Workflow, config TestConfig) {
	// Test vars
	t.Run("init", func(t *testing.T) {
		err := workflowItem.Init(workflows.Metadata{
			Base: metadata.Base{Properties: props},
		})
		assert.NoError(t, err)
	})

	// Everything is within the same task since the workflow needs to persist between operations
	if config.HasOperation("start") {
		t.Run("start", func(t *testing.T) {
			testLogger.Info("Start test running...")
			req := &workflows.StartRequest{
				Input:        10, // Time that the activity within the workflow runs for
				WorkflowName: "TestWorkflow",
			}
			req.WorkflowReference.InstanceID = "TestID"
			req.Options = make(map[string]string)
			req.Options["task_queue"] = "TestTaskQueue"
			wf, err := workflowItem.Start(context.Background(), req)
			assert.NoError(t, err)
			resp, err := workflowItem.Get(context.Background(), wf)
			assert.NoError(t, err)
			assert.Equal(t, "Running", resp.Metadata["status"])
			time.Sleep(5 * time.Second)
			resp, err = workflowItem.Get(context.Background(), wf)
			assert.NoError(t, err)
			assert.Equal(t, resp.Metadata["status"], "Running")
			err = workflowItem.Terminate(context.Background(), wf)
			assert.NoError(t, err)
			resp, err = workflowItem.Get(context.Background(), wf)
			assert.NoError(t, err)
			assert.Equal(t, "Terminated", resp.Metadata["status"])
		})
		testLogger.Info("Start test done.")
	}
}
