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

type mockThrowErrorClient struct {
	zbc.Client
	cmd1 *mockThrowErrorCommandStep1
}

type mockThrowErrorCommandStep1 struct {
	commands.ThrowErrorCommandStep1
	cmd2   *mockThrowErrorCommandStep2
	jobKey int64
}

type mockThrowErrorCommandStep2 struct {
	commands.ThrowErrorCommandStep2
	cmd3      *mockDispatchThrowErrorCommand
	errorCode string
}

type mockDispatchThrowErrorCommand struct {
	commands.DispatchThrowErrorCommand
	errorMessage string
}

func (mc *mockThrowErrorClient) NewThrowErrorCommand() commands.ThrowErrorCommandStep1 {
	mc.cmd1 = &mockThrowErrorCommandStep1{
		cmd2: &mockThrowErrorCommandStep2{
			cmd3: &mockDispatchThrowErrorCommand{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockThrowErrorCommandStep1) JobKey(jobKey int64) commands.ThrowErrorCommandStep2 {
	cmd1.jobKey = jobKey

	return cmd1.cmd2
}

func (cmd2 *mockThrowErrorCommandStep2) ErrorCode(errorCode string) commands.DispatchThrowErrorCommand {
	cmd2.errorCode = errorCode

	return cmd2.cmd3
}

func (cmd3 *mockDispatchThrowErrorCommand) ErrorMessage(errorMessage string) commands.DispatchThrowErrorCommand {
	cmd3.errorMessage = errorMessage

	return cmd3
}

func (cmd3 *mockDispatchThrowErrorCommand) Send(context.Context) (*pb.ThrowErrorResponse, error) {
	return &pb.ThrowErrorResponse{}, nil
}

func TestThrowError(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobKey is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: ThrowErrorOperation}
		_, err := cmd.Invoke(context.TODO(), req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("errorCode is mandatory", func(t *testing.T) {
		payload := throwErrorPayload{
			JobKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: ThrowErrorOperation}
		_, err = cmd.Invoke(context.TODO(), req)
		assert.Error(t, err, ErrMissingErrorCode)
	})

	t.Run("throw an error", func(t *testing.T) {
		payload := throwErrorPayload{
			JobKey:       new(int64),
			ErrorCode:    "a",
			ErrorMessage: "b",
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: ThrowErrorOperation}

		var mc mockThrowErrorClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(context.TODO(), req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.JobKey, mc.cmd1.jobKey)
		assert.Equal(t, payload.ErrorCode, mc.cmd1.cmd2.errorCode)
		assert.Equal(t, payload.ErrorMessage, mc.cmd1.cmd2.cmd3.errorMessage)
	})
}
