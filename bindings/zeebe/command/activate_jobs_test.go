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

	"github.com/stretchr/testify/assert"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/entities"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type mockActivateJobsClient struct {
	zbc.Client
	cmd1 *mockActivateJobsCommandStep1
}

type mockActivateJobsCommandStep1 struct {
	commands.ActivateJobsCommandStep1
	cmd2    *mockActivateJobsCommandStep2
	jobType string
}

type mockActivateJobsCommandStep2 struct {
	commands.ActivateJobsCommandStep2
	cmd3              *mockActivateJobsCommandStep3
	maxJobsToActivate int32
}

type mockActivateJobsCommandStep3 struct {
	commands.ActivateJobsCommandStep3
	timeout        time.Duration
	workerName     string
	fetchVariables []string
}

func (mc *mockActivateJobsClient) NewActivateJobsCommand() commands.ActivateJobsCommandStep1 {
	mc.cmd1 = &mockActivateJobsCommandStep1{
		cmd2: &mockActivateJobsCommandStep2{
			cmd3: &mockActivateJobsCommandStep3{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockActivateJobsCommandStep1) JobType(jobType string) commands.ActivateJobsCommandStep2 {
	cmd1.jobType = jobType

	return cmd1.cmd2
}

func (cmd2 *mockActivateJobsCommandStep2) MaxJobsToActivate(maxJobsToActivate int32) commands.ActivateJobsCommandStep3 {
	cmd2.maxJobsToActivate = maxJobsToActivate

	return cmd2.cmd3
}

func (cmd3 *mockActivateJobsCommandStep3) Timeout(timeout time.Duration) commands.ActivateJobsCommandStep3 {
	cmd3.timeout = timeout

	return cmd3
}

func (cmd3 *mockActivateJobsCommandStep3) WorkerName(workerName string) commands.ActivateJobsCommandStep3 {
	cmd3.workerName = workerName

	return cmd3
}

func (cmd3 *mockActivateJobsCommandStep3) FetchVariables(fetchVariables ...string) commands.ActivateJobsCommandStep3 {
	cmd3.fetchVariables = fetchVariables

	return cmd3
}

func (cmd3 *mockActivateJobsCommandStep3) Send(context.Context) ([]entities.Job, error) {
	return []entities.Job{}, nil
}

func TestActivateJobs(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobType is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: activateJobsOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingJobType)
	})

	t.Run("maxJobsToActivate is mandatory", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType: "a",
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}
		_, err = message.Invoke(req)
		assert.Error(t, err, ErrMissingMaxJobsToActivate)
	})

	t.Run("activate jobs with mandatory fields", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType:           "a",
			MaxJobsToActivate: new(int32),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}

		var mc mockActivateJobsClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, payload.JobType, mc.cmd1.jobType)
		assert.Equal(t, *payload.MaxJobsToActivate, mc.cmd1.cmd2.maxJobsToActivate)
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
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: activateJobsOperation}

		var mc mockActivateJobsClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, payload.JobType, mc.cmd1.jobType)
		assert.Equal(t, *payload.MaxJobsToActivate, mc.cmd1.cmd2.maxJobsToActivate)
		assert.Equal(t, payload.Timeout.Duration, mc.cmd1.cmd2.cmd3.timeout)
		assert.Equal(t, payload.WorkerName, mc.cmd1.cmd2.cmd3.workerName)
		assert.Equal(t, []string{"a", "b", "c"}, mc.cmd1.cmd2.cmd3.fetchVariables)
	})
}
