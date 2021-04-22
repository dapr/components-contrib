// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

type mockCreateInstanceClient struct {
	zbc.Client
	cmd1 *mockCreateInstanceCommandStep1
}

type mockCreateInstanceCommandStep1 struct {
	commands.CreateInstanceCommandStep1
	cmd2          *mockCreateInstanceCommandStep2
	bpmnProcessID string
	workflowKey   int64
}

type mockCreateInstanceCommandStep2 struct {
	commands.CreateInstanceCommandStep2
	cmd3          *mockCreateInstanceCommandStep3
	version       int32
	latestVersion bool
}

type mockCreateInstanceCommandStep3 struct {
	commands.CreateInstanceCommandStep3
	variables interface{}
}

func (mc *mockCreateInstanceClient) NewCreateInstanceCommand() commands.CreateInstanceCommandStep1 {
	mc.cmd1 = &mockCreateInstanceCommandStep1{
		cmd2: &mockCreateInstanceCommandStep2{
			cmd3: &mockCreateInstanceCommandStep3{},
		},
	}

	return mc.cmd1
}

//nolint // BPMNProcessId comes from the Zeebe client API and cannot be written as BPMNProcessID
func (cmd1 *mockCreateInstanceCommandStep1) BPMNProcessId(bpmnProcessID string) commands.CreateInstanceCommandStep2 {
	cmd1.bpmnProcessID = bpmnProcessID

	return cmd1.cmd2
}

func (cmd1 *mockCreateInstanceCommandStep1) WorkflowKey(workflowKey int64) commands.CreateInstanceCommandStep3 {
	cmd1.workflowKey = workflowKey

	return cmd1.cmd2.cmd3
}

func (cmd2 *mockCreateInstanceCommandStep2) Version(version int32) commands.CreateInstanceCommandStep3 {
	cmd2.version = version

	return cmd2.cmd3
}

func (cmd2 *mockCreateInstanceCommandStep2) LatestVersion() commands.CreateInstanceCommandStep3 {
	cmd2.latestVersion = true

	return cmd2.cmd3
}

func (cmd3 *mockCreateInstanceCommandStep3) VariablesFromObject(variables interface{}) (commands.CreateInstanceCommandStep3, error) {
	cmd3.variables = variables

	return cmd3, nil
}

func (cmd3 *mockCreateInstanceCommandStep3) Send(context.Context) (*pb.CreateWorkflowInstanceResponse, error) {
	return &pb.CreateWorkflowInstanceResponse{}, nil
}

func TestCreateInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("bpmnProcessId and workflowKey are not allowed at the same time", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
			WorkflowKey:   new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Error(t, err, ErrAmbiguousCreationVars)
	})

	t.Run("either bpmnProcessId or workflowKey must be given", func(t *testing.T) {
		payload := createInstancePayload{}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Error(t, err, ErrMissingCreationVars)
	})

	t.Run("create command with bpmnProcessId and specific version", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
			Version:       new(int32),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, payload.BpmnProcessID, mc.cmd1.bpmnProcessID)
		assert.Equal(t, *payload.Version, mc.cmd1.cmd2.version)
	})

	t.Run("create command with bpmnProcessId and latest version", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, payload.BpmnProcessID, mc.cmd1.bpmnProcessID)
		assert.Equal(t, true, mc.cmd1.cmd2.latestVersion)
	})

	t.Run("create command with workflowKey", func(t *testing.T) {
		payload := createInstancePayload{
			WorkflowKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.WorkflowKey, mc.cmd1.workflowKey)
	})

	t.Run("create command with variables", func(t *testing.T) {
		payload := createInstancePayload{
			WorkflowKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: createInstanceOperation}

		var mc mockCreateInstanceClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.WorkflowKey, mc.cmd1.workflowKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.cmd3.variables)
	})
}
