// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
)

type MockCancelInstanceCommandStep1 struct {
	mock.Mock
}

type MockCancelInstanceCommandStep2 struct {
	mock.Mock
}

func (mc *MockClient) NewCancelInstanceCommand() commands.CancelInstanceStep1 {
	return mc.Called().Get(0).(commands.CancelInstanceStep1)
}

func (cmd1 *MockCancelInstanceCommandStep1) WorkflowInstanceKey(workflowInstanceKey int64) commands.DispatchCancelWorkflowInstanceCommand {
	return cmd1.Called(workflowInstanceKey).Get(0).(commands.DispatchCancelWorkflowInstanceCommand)
}

func (cmd2 *MockCancelInstanceCommandStep2) Send(context context.Context) (*pb.CancelWorkflowInstanceResponse, error) {
	args := cmd2.Called(context)

	return args.Get(0).(*pb.CancelWorkflowInstanceResponse), args.Error(1)
}

func TestCancelInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("workflowInstanceKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: cancelInstanceOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, missingWorkflowInstanceKeyErrorMsg)
	})

	t.Run("cancel a command", func(t *testing.T) {
		payload := cancelInstancePayload{
			WorkflowInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: cancelInstanceOperation}

		mc := new(MockClient)
		cmd1 := new(MockCancelInstanceCommandStep1)
		cmd2 := new(MockCancelInstanceCommandStep2)

		mc.On("NewCancelInstanceCommand").Return(cmd1)
		cmd1.On("WorkflowInstanceKey", *payload.WorkflowInstanceKey).Return(cmd2)
		cmd2.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CancelWorkflowInstanceResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
	})
}
