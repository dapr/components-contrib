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
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
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
	mc.cmd1 = new(mockSetVariablesCommandStep1)
	mc.cmd1.cmd2 = new(mockSetVariablesCommandStep2)
	mc.cmd1.cmd2.cmd3 = new(mockDispatchSetVariablesCommand)

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
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: setVariablesOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingElementInstanceKey)
	})

	t.Run("variables is mandatory", func(t *testing.T) {
		payload := setVariablesPayload{
			ElementInstanceKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}
		_, err = message.Invoke(req)
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
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}

		var mc mockSetVariableClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

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
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: setVariablesOperation}

		var mc mockSetVariableClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		assert.Equal(t, *payload.ElementInstanceKey, mc.cmd1.elementInstanceKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.variables)
		assert.Equal(t, true, mc.cmd1.cmd2.cmd3.local)
	})
}
