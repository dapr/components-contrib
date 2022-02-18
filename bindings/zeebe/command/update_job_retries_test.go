/*
Copyright 2022 The Dapr Authors
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

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type mockUpdateJobRetriesClient struct {
	zbc.Client
	cmd1 *mockUpdateJobRetriesCommandStep1
}

type mockUpdateJobRetriesCommandStep1 struct {
	commands.UpdateJobRetriesCommandStep1
	cmd2   *mockUpdateJobRetriesCommandStep2
	jobKey int64
}

type mockUpdateJobRetriesCommandStep2 struct {
	commands.UpdateJobRetriesCommandStep2
	retries int32
}

func (mc *mockUpdateJobRetriesClient) NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1 {
	mc.cmd1 = &mockUpdateJobRetriesCommandStep1{
		cmd2: &mockUpdateJobRetriesCommandStep2{},
	}

	return mc.cmd1
}

func (cmd1 *mockUpdateJobRetriesCommandStep1) JobKey(jobKey int64) commands.UpdateJobRetriesCommandStep2 {
	cmd1.jobKey = jobKey

	return cmd1.cmd2
}

func (cmd2 *mockUpdateJobRetriesCommandStep2) Retries(retries int32) commands.DispatchUpdateJobRetriesCommand {
	cmd2.retries = retries

	return cmd2
}

func (cmd2 *mockUpdateJobRetriesCommandStep2) Send(context.Context) (*pb.UpdateJobRetriesResponse, error) {
	return &pb.UpdateJobRetriesResponse{}, nil
}

func TestUpdateJobRetries(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: UpdateJobRetriesOperation}
		_, err := cmd.Invoke(req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("update job retries", func(t *testing.T) {
		payload := updateJobRetriesPayload{
			JobKey:  new(int64),
			Retries: new(int32),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: UpdateJobRetriesOperation}

		var mc mockUpdateJobRetriesClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.JobKey, mc.cmd1.jobKey)
		assert.Equal(t, *payload.Retries, mc.cmd1.cmd2.retries)
	})
}
