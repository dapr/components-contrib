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

type MockThrowErrorCommandStep1 struct {
	mock.Mock
}

type MockThrowErrorCommandStep2 struct {
	mock.Mock
}

type MockThrowErrorCommandStep3 struct {
	mock.Mock
}

func (mc *MockClient) NewThrowErrorCommand() commands.ThrowErrorCommandStep1 {
	return mc.Called().Get(0).(commands.ThrowErrorCommandStep1)
}

func (cmd1 *MockThrowErrorCommandStep1) JobKey(jobKey int64) commands.ThrowErrorCommandStep2 {
	return cmd1.Called(jobKey).Get(0).(commands.ThrowErrorCommandStep2)
}

func (cmd2 *MockThrowErrorCommandStep2) ErrorCode(errorCode string) commands.DispatchThrowErrorCommand {
	return cmd2.Called(errorCode).Get(0).(commands.DispatchThrowErrorCommand)
}

func (cmd3 *MockThrowErrorCommandStep3) ErrorMessage(errorMessage string) commands.DispatchThrowErrorCommand {
	return cmd3.Called(errorMessage).Get(0).(commands.DispatchThrowErrorCommand)
}

func (cmd3 *MockThrowErrorCommandStep3) Send(context context.Context) (*pb.ThrowErrorResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.ThrowErrorResponse), args.Error(1)
}

func TestThrowError(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: throwErrorOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, missingJobKeyErrorMsg)
	})

	t.Run("errorCode is mandatory", func(t *testing.T) {
		payload := throwErrorPayload{
			JobKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: throwErrorOperation}
		_, err = message.Invoke(req)
		assert.Error(t, err, missingErrorCodeErrorMsg)
	})

	t.Run("throw an error", func(t *testing.T) {
		payload := throwErrorPayload{
			JobKey:       new(int64),
			ErrorCode:    "a",
			ErrorMessage: "b",
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: throwErrorOperation}

		mc := new(MockClient)
		cmd1 := new(MockThrowErrorCommandStep1)
		cmd2 := new(MockThrowErrorCommandStep2)
		cmd3 := new(MockThrowErrorCommandStep3)

		mc.On("NewThrowErrorCommand").Return(cmd1)
		cmd1.On("JobKey", *payload.JobKey).Return(cmd2)
		cmd2.On("ErrorCode", payload.ErrorCode).Return(cmd3)
		cmd3.On("ErrorMessage", payload.ErrorMessage).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.ThrowErrorResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
