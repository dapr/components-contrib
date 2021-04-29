// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type mockCancelInstanceClient struct {
	zbc.Client
	cmd1 *mockCancelInstanceStep1
}

type mockCancelInstanceStep1 struct {
	commands.CancelInstanceStep1
	cmd2 *mockDispatchCancelWorkflowInstanceCommand
}

type mockDispatchCancelWorkflowInstanceCommand struct {
	commands.DispatchCancelWorkflowInstanceCommand
	workflowInstanceKey int64
}

func (mc *mockCancelInstanceClient) NewCancelInstanceCommand() commands.CancelInstanceStep1 {
	mc.cmd1 = &mockCancelInstanceStep1{
		cmd2: &mockDispatchCancelWorkflowInstanceCommand{},
	}

	return mc.cmd1
}

func (cmd1 *mockCancelInstanceStep1) WorkflowInstanceKey(workflowInstanceKey int64) commands.DispatchCancelWorkflowInstanceCommand {
	cmd1.cmd2.workflowInstanceKey = workflowInstanceKey

	return cmd1.cmd2
}

func (cmd2 *mockDispatchCancelWorkflowInstanceCommand) Send(context.Context) (*pb.CancelWorkflowInstanceResponse, error) {
	return &pb.CancelWorkflowInstanceResponse{}, nil
}

func TestCancelInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("workflowInstanceKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: cancelInstanceOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingWorkflowInstanceKey)
	})

	t.Run("cancel a command", func(t *testing.T) {
		payload := cancelInstancePayload{
			WorkflowInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: cancelInstanceOperation}

		var mc mockCancelInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.WorkflowInstanceKey, mc.cmd1.cmd2.workflowInstanceKey)
	})
}
