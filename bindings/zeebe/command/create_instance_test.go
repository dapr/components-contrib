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

type MockCreateInstanceCommandStep1 struct {
	mock.Mock
}

type MockCreateInstanceCommandStep2 struct {
	mock.Mock
}

type MockCreateInstanceCommandStep3 struct {
	commands.CreateInstanceCommandStep3
	mock.Mock
}

func (mc *MockClient) NewCreateInstanceCommand() commands.CreateInstanceCommandStep1 {
	return mc.Called().Get(0).(commands.CreateInstanceCommandStep1)
}

//nolint // BPMNProcessId comes from the Zeebe client API and cannot be written as BPMNProcessID
func (cmd1 *MockCreateInstanceCommandStep1) BPMNProcessId(bpmnProcessID string) commands.CreateInstanceCommandStep2 {
	return cmd1.Called(bpmnProcessID).Get(0).(commands.CreateInstanceCommandStep2)
}

func (cmd1 *MockCreateInstanceCommandStep1) WorkflowKey(workflowKey int64) commands.CreateInstanceCommandStep3 {
	return cmd1.Called(workflowKey).Get(0).(commands.CreateInstanceCommandStep3)
}

func (cmd2 *MockCreateInstanceCommandStep2) Version(version int32) commands.CreateInstanceCommandStep3 {
	return cmd2.Called(version).Get(0).(commands.CreateInstanceCommandStep3)
}

func (cmd2 *MockCreateInstanceCommandStep2) LatestVersion() commands.CreateInstanceCommandStep3 {
	return cmd2.Called().Get(0).(commands.CreateInstanceCommandStep3)
}

func (cmd3 *MockCreateInstanceCommandStep3) VariablesFromObject(variables interface{}) (commands.CreateInstanceCommandStep3, error) {
	args := cmd3.Called(variables)

	return args.Get(0).(commands.CreateInstanceCommandStep3), args.Error(1)
}

func (cmd3 *MockCreateInstanceCommandStep3) Send(context context.Context) (*pb.CreateWorkflowInstanceResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.CreateWorkflowInstanceResponse), args.Error(1)
}

func TestCreateInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("bpmnProcessId and workflowKey are not allowed at the same time", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
			WorkflowKey:   new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		mc := new(MockClient)
		create1 := new(MockCreateInstanceCommandStep1)

		mc.On("NewCreateInstanceCommand").Return(create1)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Error(t, err, ErrAmbiguousCreationVars)
	})

	t.Run("create command with bpmnProcessId and specific version", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
			Version:       new(int32),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		mc := new(MockClient)
		cmd1 := new(MockCreateInstanceCommandStep1)
		cmd2 := new(MockCreateInstanceCommandStep2)
		cmd3 := new(MockCreateInstanceCommandStep3)

		mc.On("NewCreateInstanceCommand").Return(cmd1)
		cmd1.On("BPMNProcessId", payload.BpmnProcessID).Return(cmd2)
		cmd2.On("Version", *payload.Version).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CreateWorkflowInstanceResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})

	t.Run("create command with bpmnProcessId and latest version", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		mc := new(MockClient)
		cmd1 := new(MockCreateInstanceCommandStep1)
		cmd2 := new(MockCreateInstanceCommandStep2)
		cmd3 := new(MockCreateInstanceCommandStep3)

		mc.On("NewCreateInstanceCommand").Return(cmd1)
		cmd1.On("BPMNProcessId", payload.BpmnProcessID).Return(cmd2)
		cmd2.On("LatestVersion").Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CreateWorkflowInstanceResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})

	t.Run("create command with workflowKey", func(t *testing.T) {
		payload := createInstancePayload{
			WorkflowKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		mc := new(MockClient)
		cmd1 := new(MockCreateInstanceCommandStep1)
		cmd2 := new(MockCreateInstanceCommandStep2)
		cmd3 := new(MockCreateInstanceCommandStep3)

		mc.On("NewCreateInstanceCommand").Return(cmd1)
		cmd1.On("WorkflowKey", *payload.WorkflowKey).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CreateWorkflowInstanceResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})

	t.Run("create command with variables", func(t *testing.T) {
		payload := createInstancePayload{
			WorkflowKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		mc := new(MockClient)
		cmd1 := new(MockCreateInstanceCommandStep1)
		cmd2 := new(MockCreateInstanceCommandStep2)
		cmd3 := new(MockCreateInstanceCommandStep3)

		mc.On("NewCreateInstanceCommand").Return(cmd1)
		cmd1.On("WorkflowKey", *payload.WorkflowKey).Return(cmd3)
		cmd3.On("VariablesFromObject", payload.Variables).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CreateWorkflowInstanceResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
