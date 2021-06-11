// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

type mockCompleteJobClient struct {
	zbc.Client
	cmd1 *mockCompleteJobCommandStep1
}

type mockCompleteJobCommandStep1 struct {
	commands.CompleteJobCommandStep1
	cmd2   *mockCompleteJobCommandStep2
	jobKey int64
}

type mockCompleteJobCommandStep2 struct {
	commands.CompleteJobCommandStep2
	cmd3      *mockDispatchCompleteJobCommand
	variables interface{}
}

type mockDispatchCompleteJobCommand struct {
	commands.DispatchCompleteJobCommand
}

func (mc *mockCompleteJobClient) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	mc.cmd1 = &mockCompleteJobCommandStep1{
		cmd2: &mockCompleteJobCommandStep2{
			cmd3: &mockDispatchCompleteJobCommand{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockCompleteJobCommandStep1) JobKey(jobKey int64) commands.CompleteJobCommandStep2 {
	cmd1.jobKey = jobKey

	return cmd1.cmd2
}

func (cmd2 *mockCompleteJobCommandStep2) VariablesFromObject(variables interface{}) (commands.DispatchCompleteJobCommand, error) {
	cmd2.variables = variables

	return cmd2.cmd3, nil
}

func (cmd3 *mockDispatchCompleteJobCommand) Send(context.Context) (*pb.CompleteJobResponse, error) {
	return &pb.CompleteJobResponse{}, nil
}

func TestCompleteJob(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("elementInstanceKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: completeJobOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingJobKey)
	})

	t.Run("complete a job", func(t *testing.T) {
		payload := completeJobPayload{
			JobKey: new(int64),
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: completeJobOperation}

		var mc mockCompleteJobClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.NoError(t, err)

		assert.Equal(t, *payload.JobKey, mc.cmd1.jobKey)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.variables)
	})
}
