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

type MockFailJobCommandStep1 struct {
	mock.Mock
}

type MockFailJobCommandStep2 struct {
	mock.Mock
}

type MockFailJobCommandStep3 struct {
	mock.Mock
}

func (mc *MockClient) NewFailJobCommand() commands.FailJobCommandStep1 {
	return mc.Called().Get(0).(commands.FailJobCommandStep1)
}

func (cmd1 *MockFailJobCommandStep1) JobKey(jobKey int64) commands.FailJobCommandStep2 {
	return cmd1.Called(jobKey).Get(0).(commands.FailJobCommandStep2)
}

func (cmd2 *MockFailJobCommandStep2) Retries(retries int32) commands.FailJobCommandStep3 {
	return cmd2.Called(retries).Get(0).(commands.FailJobCommandStep3)
}

func (cmd3 *MockFailJobCommandStep3) ErrorMessage(errorMessage string) commands.FailJobCommandStep3 {
	return cmd3.Called(errorMessage).Get(0).(commands.FailJobCommandStep3)
}

func (cmd3 *MockFailJobCommandStep3) Send(context context.Context) (*pb.FailJobResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.FailJobResponse), args.Error(1)
}

func TestFailJob(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: failJobOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, missingJobKeyErrorMsg)
	})

	t.Run("retries is mandatory", func(t *testing.T) {
		payload := failJobPayload{
			JobKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: failJobOperation}
		_, err = message.Invoke(req)
		assert.Error(t, err, missingRetriesErrorMsg)
	})

	t.Run("fail a job", func(t *testing.T) {
		payload := failJobPayload{
			JobKey:       new(int64),
			Retries:      new(int32),
			ErrorMessage: "a",
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: failJobOperation}

		mc := new(MockClient)
		cmd1 := new(MockFailJobCommandStep1)
		cmd2 := new(MockFailJobCommandStep2)
		cmd3 := new(MockFailJobCommandStep3)

		mc.On("NewFailJobCommand").Return(cmd1)
		cmd1.On("JobKey", *payload.JobKey).Return(cmd2)
		cmd2.On("Retries", *payload.Retries).Return(cmd3)
		cmd3.On("ErrorMessage", payload.ErrorMessage).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.FailJobResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
