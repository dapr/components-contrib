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

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type mockFailJobClient struct {
	zbc.Client
	cmd1 *mockFailJobCommandStep1
}

type mockFailJobCommandStep1 struct {
	commands.FailJobCommandStep1
	cmd2   *mockFailJobCommandStep2
	jobKey int64
}

type mockFailJobCommandStep2 struct {
	commands.FailJobCommandStep2
	cmd3    *mockFailJobCommandStep3
	retries int32
}

type mockFailJobCommandStep3 struct {
	commands.FailJobCommandStep3
	errorMessage string
}

func (mc *mockFailJobClient) NewFailJobCommand() commands.FailJobCommandStep1 {
	mc.cmd1 = &mockFailJobCommandStep1{
		cmd2: &mockFailJobCommandStep2{
			cmd3: &mockFailJobCommandStep3{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockFailJobCommandStep1) JobKey(jobKey int64) commands.FailJobCommandStep2 {
	cmd1.jobKey = jobKey

	return cmd1.cmd2
}

func (cmd2 *mockFailJobCommandStep2) Retries(retries int32) commands.FailJobCommandStep3 {
	cmd2.retries = retries

	return cmd2.cmd3
}

func (cmd3 *mockFailJobCommandStep3) ErrorMessage(errorMessage string) commands.FailJobCommandStep3 {
	cmd3.errorMessage = errorMessage

	return cmd3
}

func (cmd3 *mockFailJobCommandStep3) Send(context.Context) (*pb.FailJobResponse, error) {
	return &pb.FailJobResponse{}, nil
}

func TestFailJob(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: FailJobOperation}
		_, err := cmd.Invoke(req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("retries is mandatory", func(t *testing.T) {
		payload := failJobPayload{
			JobKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: FailJobOperation}
		_, err = cmd.Invoke(req)
		assert.Error(t, err, ErrMissingRetries)
	})

	t.Run("fail a job", func(t *testing.T) {
		payload := failJobPayload{
			JobKey:       new(int64),
			Retries:      new(int32),
			ErrorMessage: "a",
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: FailJobOperation}

		var mc mockFailJobClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.JobKey, mc.cmd1.jobKey)
		assert.Equal(t, *payload.Retries, mc.cmd1.cmd2.retries)
		assert.Equal(t, payload.ErrorMessage, mc.cmd1.cmd2.cmd3.errorMessage)
	})
}
