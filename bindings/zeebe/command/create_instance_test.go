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

type mockCreateInstanceClient struct {
	zbc.Client
	cmd1 *mockCreateInstanceCommandStep1
}

type mockCreateInstanceCommandStep1 struct {
	commands.CreateInstanceCommandStep1
	cmd2                 *mockCreateInstanceCommandStep2
	bpmnProcessID        string
	processDefinitionKey int64
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

func (cmd1 *mockCreateInstanceCommandStep1) ProcessDefinitionKey(processDefinitionKey int64) commands.CreateInstanceCommandStep3 {
	cmd1.processDefinitionKey = processDefinitionKey

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

func (cmd3 *mockCreateInstanceCommandStep3) Send(context.Context) (*pb.CreateProcessInstanceResponse, error) {
	return &pb.CreateProcessInstanceResponse{}, nil
}

func TestCreateInstance(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("bpmnProcessId and processDefinitionKey are not allowed at the same time", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID:        "some-id",
			ProcessDefinitionKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.Error(t, err, ErrAmbiguousCreationVars)
	})

	t.Run("either bpmnProcessId or processDefinitionKey must be given", func(t *testing.T) {
		payload := createInstancePayload{}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.Error(t, err, ErrMissingCreationVars)
	})

	t.Run("create command with bpmnProcessId and specific version", func(t *testing.T) {
		payload := createInstancePayload{
			BpmnProcessID: "some-id",
			Version:       new(int32),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
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

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, payload.BpmnProcessID, mc.cmd1.bpmnProcessID)
		assert.Equal(t, true, mc.cmd1.cmd2.latestVersion)
	})

	t.Run("create command with processDefinitionKey", func(t *testing.T) {
		payload := createInstancePayload{
			ProcessDefinitionKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.ProcessDefinitionKey, mc.cmd1.processDefinitionKey)
	})

	t.Run("create command with variables", func(t *testing.T) {
		payload := createInstancePayload{
			ProcessDefinitionKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: CreateInstanceOperation}

		var mc mockCreateInstanceClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.ProcessDefinitionKey, mc.cmd1.processDefinitionKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.cmd3.variables)
	})
}
