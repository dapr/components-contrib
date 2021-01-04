// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
)

type MockActivateJobsCommandStep1 struct {
	mock.Mock
}

type MockActivateJobsCommandStep2 struct {
	mock.Mock
}

type MockActivateJobsCommandStep3 struct {
	mock.Mock
}

func (mc *MockClient) NewActivateJobsCommand() commands.ActivateJobsCommandStep1 {
	return mc.Called().Get(0).(commands.ActivateJobsCommandStep1)
}

func (cmd1 *MockActivateJobsCommandStep1) JobType(jobType string) commands.ActivateJobsCommandStep2 {
	return cmd1.Called(jobType).Get(0).(commands.ActivateJobsCommandStep2)
}

func (cmd2 *MockActivateJobsCommandStep2) MaxJobsToActivate(maxJobsToActivate int32) commands.ActivateJobsCommandStep3 {
	return cmd2.Called(maxJobsToActivate).Get(0).(commands.ActivateJobsCommandStep3)
}

func (cmd3 *MockActivateJobsCommandStep3) Timeout(timeout time.Duration) commands.ActivateJobsCommandStep3 {
	return cmd3.Called(timeout).Get(0).(commands.ActivateJobsCommandStep3)
}

func (cmd3 *MockActivateJobsCommandStep3) WorkerName(workerName string) commands.ActivateJobsCommandStep3 {
	return cmd3.Called(workerName).Get(0).(commands.ActivateJobsCommandStep3)
}

func (cmd3 *MockActivateJobsCommandStep3) FetchVariables(fetchVariables ...string) commands.ActivateJobsCommandStep3 {
	return cmd3.Called(fetchVariables).Get(0).(commands.ActivateJobsCommandStep3)
}

func (cmd3 *MockActivateJobsCommandStep3) Send(context context.Context) ([]entities.Job, error) {
	args := cmd3.Called(context)

	return cmd3.Called(context).Get(0).([]entities.Job), args.Error(1)
}

func TestActivateJobs(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobType is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: activateJobsOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, missingJobTypeErrorMsg)
	})

	t.Run("maxJobsToActivate is mandatory", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType: "a",
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}
		_, err = message.Invoke(req)
		assert.Error(t, err, missingMaxJobsToActivateErrorMsg)
	})

	t.Run("activate jobs with mandatory fields", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType:           "a",
			MaxJobsToActivate: new(int32),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}

		mc := new(MockClient)
		cmd1 := new(MockActivateJobsCommandStep1)
		cmd2 := new(MockActivateJobsCommandStep2)
		cmd3 := new(MockActivateJobsCommandStep3)

		mc.On("NewActivateJobsCommand").Return(cmd1)
		cmd1.On("JobType", payload.JobType).Return(cmd2)
		cmd2.On("MaxJobsToActivate", *payload.MaxJobsToActivate).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return([]entities.Job{}, nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})

	t.Run("send message with optional fields", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType:           "a",
			MaxJobsToActivate: new(int32),
			Timeout:           contrib_metadata.Duration{Duration: 1 * time.Second},
			WorkerName:        "b",
			FetchVariables:    []string{"a", "b", "c"},
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}

		mc := new(MockClient)
		cmd1 := new(MockActivateJobsCommandStep1)
		cmd2 := new(MockActivateJobsCommandStep2)
		cmd3 := new(MockActivateJobsCommandStep3)

		mc.On("NewActivateJobsCommand").Return(cmd1)
		cmd1.On("JobType", payload.JobType).Return(cmd2)
		cmd2.On("MaxJobsToActivate", *payload.MaxJobsToActivate).Return(cmd3)
		cmd3.On("Timeout", payload.Timeout.Duration).Return(cmd3)
		cmd3.On("WorkerName", payload.WorkerName).Return(cmd3)
		cmd3.On("FetchVariables", []string{"a", "b", "c"}).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return([]entities.Job{}, nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
