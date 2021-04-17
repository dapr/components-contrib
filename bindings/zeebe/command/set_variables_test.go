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

type MockSetVariablesCommandStep1 struct {
	mock.Mock
}

type MockSetVariablesCommandStep2 struct {
	commands.SetVariablesCommandStep2
	mock.Mock
}

type MockSetVariablesCommandStep3 struct {
	mock.Mock
}

func (mc *MockClient) NewSetVariablesCommand() commands.SetVariablesCommandStep1 {
	return mc.Called().Get(0).(commands.SetVariablesCommandStep1)
}

func (cmd1 *MockSetVariablesCommandStep1) ElementInstanceKey(elementInstanceKey int64) commands.SetVariablesCommandStep2 {
	return cmd1.Called(elementInstanceKey).Get(0).(commands.SetVariablesCommandStep2)
}

func (cmd2 *MockSetVariablesCommandStep2) VariablesFromObject(variables interface{}) (commands.DispatchSetVariablesCommand, error) {
	args := cmd2.Called(variables)

	return args.Get(0).(commands.DispatchSetVariablesCommand), args.Error(1)
}

func (cmd3 *MockSetVariablesCommandStep3) Local(local bool) commands.DispatchSetVariablesCommand {
	return cmd3.Called(local).Get(0).(commands.DispatchSetVariablesCommand)
}

func (cmd3 *MockSetVariablesCommandStep3) Send(context context.Context) (*pb.SetVariablesResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.SetVariablesResponse), args.Error(1)
}

func TestSetVariables(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("elementInstanceKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: setVariablesOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingElementInstanceKey)
	})

	t.Run("variables is mandatory", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}
		_, err = message.Invoke(req)
		assert.Error(t, err, ErrMissingVariables)
	})

	t.Run("set variables", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}

		mc := new(MockClient)
		cmd1 := new(MockSetVariablesCommandStep1)
		cmd2 := new(MockSetVariablesCommandStep2)
		cmd3 := new(MockSetVariablesCommandStep3)

		mc.On("NewSetVariablesCommand").Return(cmd1)
		cmd1.On("ElementInstanceKey", *payload.ElementInstanceKey).Return(cmd2)
		cmd2.On("VariablesFromObject", payload.Variables).Return(cmd3, nil)
		cmd3.On("Local", false).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.SetVariablesResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})

	t.Run("set local variables", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
			Local: true,
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}

		mc := new(MockClient)
		cmd1 := new(MockSetVariablesCommandStep1)
		cmd2 := new(MockSetVariablesCommandStep2)
		cmd3 := new(MockSetVariablesCommandStep3)

		mc.On("NewSetVariablesCommand").Return(cmd1)
		cmd1.On("ElementInstanceKey", *payload.ElementInstanceKey).Return(cmd2)
		cmd2.On("VariablesFromObject", payload.Variables).Return(cmd3, nil)
		cmd3.On("Local", true).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.SetVariablesResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
