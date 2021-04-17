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

type MockCompleteJobCommandStep1 struct {
	mock.Mock
}

type MockCompleteJobCommandStep2 struct {
	commands.CompleteJobCommandStep2
	mock.Mock
}

type MockCompleteJobCommandStep3 struct {
	mock.Mock
}

func (mc *MockClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return mc.Called().Get(0).(commands.CompleteJobCommandStep1)
}

func (cmd1 *MockCompleteJobCommandStep1) JobKey(jobKey int64) commands.CompleteJobCommandStep2 {
	return cmd1.Called(jobKey).Get(0).(commands.CompleteJobCommandStep2)
}

func (cmd2 *MockCompleteJobCommandStep2) VariablesFromObject(variables interface{}) (commands.DispatchCompleteJobCommand, error) {
	args := cmd2.Called(variables)

	return args.Get(0).(commands.DispatchCompleteJobCommand), args.Error(1)
}

func (cmd3 *MockCompleteJobCommandStep3) Send(context context.Context) (*pb.CompleteJobResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.CompleteJobResponse), args.Error(1)
}

func TestCompleteJob(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("elementInstanceKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: completeJobOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("complete a job", func(t *testing.T) {
		payload := completeJobPayload{
			JobKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: completeJobOperation}

		mc := new(MockClient)
		cmd1 := new(MockCompleteJobCommandStep1)
		cmd2 := new(MockCompleteJobCommandStep2)
		cmd3 := new(MockCompleteJobCommandStep3)

		mc.On("NewCompleteJobCommand").Return(cmd1)
		cmd1.On("JobKey", *payload.JobKey).Return(cmd2)
		cmd2.On("VariablesFromObject", payload.Variables).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.CompleteJobResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
