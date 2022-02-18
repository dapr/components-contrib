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

type mockSetVariableClient struct {
	zbc.Client
	cmd1 *mockSetVariablesCommandStep1
}

type mockSetVariablesCommandStep1 struct {
	commands.SetVariablesCommandStep1
	cmd2               *mockSetVariablesCommandStep2
	elementInstanceKey int64
}

type mockSetVariablesCommandStep2 struct {
	commands.SetVariablesCommandStep2
	cmd3      *mockDispatchSetVariablesCommand
	variables interface{}
}

type mockDispatchSetVariablesCommand struct {
	commands.DispatchSetVariablesCommand
	local bool
}

func (mc *mockSetVariableClient) NewSetVariablesCommand() commands.SetVariablesCommandStep1 {
	mc.cmd1 = &mockSetVariablesCommandStep1{
		cmd2: &mockSetVariablesCommandStep2{
			cmd3: &mockDispatchSetVariablesCommand{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockSetVariablesCommandStep1) ElementInstanceKey(elementInstanceKey int64) commands.SetVariablesCommandStep2 {
	cmd1.elementInstanceKey = elementInstanceKey

	return cmd1.cmd2
}

func (cmd2 *mockSetVariablesCommandStep2) VariablesFromObject(variables interface{}) (commands.DispatchSetVariablesCommand, error) {
	cmd2.variables = variables

	return cmd2.cmd3, nil
}

func (cmd3 *mockDispatchSetVariablesCommand) Local(local bool) commands.DispatchSetVariablesCommand {
	cmd3.local = local

	return cmd3
}

func (cmd3 *mockDispatchSetVariablesCommand) Send(context.Context) (*pb.SetVariablesResponse, error) {
	return &pb.SetVariablesResponse{}, nil
}

func TestSetVariables(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("elementInstanceKey is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: SetVariablesOperation}
		_, err := cmd.Invoke(req)
		assert.Error(t, err, ErrMissingElementInstanceKey)
	})

	t.Run("variables is mandatory", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: SetVariablesOperation}
		_, err = cmd.Invoke(req)
		assert.Error(t, err, ErrMissingVariables)
	})

	t.Run("set variables", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: SetVariablesOperation}

		var mc mockSetVariableClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.ElementInstanceKey, mc.cmd1.elementInstanceKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.variables)
		assert.Equal(t, false, mc.cmd1.cmd2.cmd3.local)
	})

	t.Run("set local variables", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
			Local: true,
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: SetVariablesOperation}

		var mc mockSetVariableClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.ElementInstanceKey, mc.cmd1.elementInstanceKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.variables)
		assert.Equal(t, true, mc.cmd1.cmd2.cmd3.local)
	})
}
