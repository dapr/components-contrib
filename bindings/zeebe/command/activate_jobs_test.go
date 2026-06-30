/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/commands"
	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"
	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
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

func TestActivateJobsPayloadUnmarshal(t *testing.T) {
	t.Run("invalid timeout produces field-scoped error", func(t *testing.T) {
		raw := `{"jobType":"worker","maxJobsToActivate":10,"timeout":"PT30S"}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
		assert.Contains(t, err.Error(), "PT30S")
		assert.Contains(t, err.Error(), "duration string")
	})

	t.Run("invalid requestTimeout produces field-scoped error", func(t *testing.T) {
		raw := `{"jobType":"worker","maxJobsToActivate":10,"requestTimeout":"2minutes"}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "requestTimeout")
		assert.Contains(t, err.Error(), "2minutes")
		assert.Contains(t, err.Error(), "duration string")
	})

	t.Run("valid Go duration strings succeed", func(t *testing.T) {
		raw := `{"jobType":"worker","maxJobsToActivate":10,"timeout":"30s","requestTimeout":"5m"}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		require.NotNil(t, p.Timeout)
		assert.Equal(t, 30*time.Second, p.Timeout.Duration)
		require.NotNil(t, p.RequestTimeout)
		assert.Equal(t, 5*time.Minute, p.RequestTimeout.Duration)
	})

	t.Run("numeric nanosecond timeout still parses correctly", func(t *testing.T) {
		// Regression guard: numeric (nanosecond) values were accepted before and must remain valid.
		raw := `{"jobType":"worker","maxJobsToActivate":10,"timeout":30000000000}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		require.NotNil(t, p.Timeout)
		assert.Equal(t, 30*time.Second, p.Timeout.Duration)
	})

	t.Run("numeric nanosecond requestTimeout still parses correctly", func(t *testing.T) {
		// Regression guard: numeric (nanosecond) values were accepted before and must remain valid.
		raw := `{"jobType":"worker","maxJobsToActivate":10,"requestTimeout":300000000000}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		require.NotNil(t, p.RequestTimeout)
		assert.Equal(t, 5*time.Minute, p.RequestTimeout.Duration)
	})

	t.Run("null timeout leaves pointer nil", func(t *testing.T) {
		raw := `{"jobType":"worker","maxJobsToActivate":10,"timeout":null}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		assert.Nil(t, p.Timeout)
	})

	t.Run("null requestTimeout leaves pointer nil", func(t *testing.T) {
		raw := `{"jobType":"worker","maxJobsToActivate":10,"requestTimeout":null}`
		var p activateJobsPayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		assert.Nil(t, p.RequestTimeout)
	})
}

func TestActivateJobs(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobType is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		payload := map[string]string{}
		data, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		req := &bindings.InvokeRequest{Operation: ActivateJobsOperation, Data: data}
		_, err := cmd.Invoke(t.Context(), req)
		require.ErrorIs(t, err, ErrMissingJobType)
	})

	t.Run("maxJobsToActivate is mandatory", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType: "a",
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: ActivateJobsOperation}
		_, err = cmd.Invoke(t.Context(), req)
		require.ErrorIs(t, err, ErrMissingMaxJobsToActivate)
	})

	t.Run("activate jobs with mandatory fields", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType:           "a",
			MaxJobsToActivate: new(int32),
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: ActivateJobsOperation}

		var mc mockActivateJobsClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(t.Context(), req)
		require.NoError(t, err)

		assert.Equal(t, payload.JobType, mc.cmd1.jobType)
		assert.Equal(t, *payload.MaxJobsToActivate, mc.cmd1.cmd2.maxJobsToActivate)
	})

	t.Run("send message with optional fields", func(t *testing.T) {
		payload := activateJobsPayload{
			JobType:           "a",
			MaxJobsToActivate: new(int32),
			Timeout:           &kitmd.Duration{Duration: 1 * time.Second},
			WorkerName:        "b",
			FetchVariables:    []string{"a", "b", "c"},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: ActivateJobsOperation}

		var mc mockActivateJobsClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(t.Context(), req)
		require.NoError(t, err)

		assert.Equal(t, payload.JobType, mc.cmd1.jobType)
		assert.Equal(t, *payload.MaxJobsToActivate, mc.cmd1.cmd2.maxJobsToActivate)
		assert.Equal(t, payload.Timeout.Duration, mc.cmd1.cmd2.cmd3.timeout)
		assert.Equal(t, payload.WorkerName, mc.cmd1.cmd2.cmd3.workerName)
		assert.Equal(t, []string{"a", "b", "c"}, mc.cmd1.cmd2.cmd3.fetchVariables)
	})
}
