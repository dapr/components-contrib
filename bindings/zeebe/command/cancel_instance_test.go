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

type mockCancelInstanceClient struct {
	zbc.Client
	cmd1 *mockCancelInstanceStep1
}

type mockCancelInstanceStep1 struct {
	commands.CancelInstanceStep1
	cmd2 *mockDispatchCancelProcessInstanceCommand
}

type mockDispatchCancelProcessInstanceCommand struct {
	commands.DispatchCancelProcessInstanceCommand
	processInstanceKey int64
}

func (mc *mockCancelInstanceClient) NewCancelInstanceCommand() commands.CancelInstanceStep1 {
	mc.cmd1 = &mockCancelInstanceStep1{
		cmd2: &mockDispatchCancelProcessInstanceCommand{},
	}

	return mc.cmd1
}

func (cmd1 *mockCancelInstanceStep1) ProcessInstanceKey(processInstanceKey int64) commands.DispatchCancelProcessInstanceCommand {
	cmd1.cmd2.processInstanceKey = processInstanceKey

	return cmd1.cmd2
}

func (cmd2 *mockDispatchCancelProcessInstanceCommand) Send(context.Context) (*pb.CancelProcessInstanceResponse, error) {
	return &pb.CancelProcessInstanceResponse{}, nil
}

func TestCancelInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("processInstanceKey is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: CancelInstanceOperation}
		_, err := cmd.Invoke(req)
		assert.Error(t, err, ErrMissingProcessInstanceKey)
	})

	t.Run("cancel a command", func(t *testing.T) {
		payload := cancelInstancePayload{
			ProcessInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CancelInstanceOperation}

		var mc mockCancelInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.ProcessInstanceKey, mc.cmd1.cmd2.processInstanceKey)
	})
}
