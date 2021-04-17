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

type MockUpdateJobRetriesCommandStep1 struct {
	mock.Mock
}

type MockUpdateJobRetriesCommandStep2 struct {
	mock.Mock
}

func (mc *MockClient) NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1 {
	return mc.Called().Get(0).(commands.UpdateJobRetriesCommandStep1)
}

func (cmd1 *MockUpdateJobRetriesCommandStep1) JobKey(jobKey int64) commands.UpdateJobRetriesCommandStep2 {
	return cmd1.Called(jobKey).Get(0).(commands.UpdateJobRetriesCommandStep2)
}

func (cmd2 *MockUpdateJobRetriesCommandStep2) Retries(retries int32) commands.DispatchUpdateJobRetriesCommand {
	return cmd2.Called(retries).Get(0).(commands.DispatchUpdateJobRetriesCommand)
}

func (cmd2 *MockUpdateJobRetriesCommandStep2) Send(context context.Context) (*pb.UpdateJobRetriesResponse, error) {
	args := cmd2.Called(context)

	return args.Get(0).(*pb.UpdateJobRetriesResponse), args.Error(1)
}

func TestUpdateJobRetries(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: updateJobRetriesOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("update job retries", func(t *testing.T) {
		payload := updateJobRetriesPayload{
			JobKey:  new(int64),
			Retries: new(int32),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: updateJobRetriesOperation}

		mc := new(MockClient)
		cmd1 := new(MockUpdateJobRetriesCommandStep1)
		cmd2 := new(MockUpdateJobRetriesCommandStep2)

		mc.On("NewUpdateJobRetriesCommand").Return(cmd1)
		cmd1.On("JobKey", *payload.JobKey).Return(cmd2)
		cmd2.On("Retries", *payload.Retries).Return(cmd2)
		cmd2.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.UpdateJobRetriesResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
	})
}
