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
	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

type mockPublishMessageClient struct {
	zbc.Client
	cmd1 *mockPublishMessageCommandStep1
}

type mockPublishMessageCommandStep1 struct {
	commands.PublishMessageCommandStep1
	cmd2        *mockPublishMessageCommandStep2
	messageName string
}

type mockPublishMessageCommandStep2 struct {
	commands.PublishMessageCommandStep2
	cmd3           *mockPublishMessageCommandStep3
	correlationKey string
}

type mockPublishMessageCommandStep3 struct {
	commands.PublishMessageCommandStep3
	messageID  string
	timeToLive time.Duration
	variables  interface{}
}

func (mc *mockPublishMessageClient) NewPublishMessageCommand() commands.PublishMessageCommandStep1 {
	mc.cmd1 = &mockPublishMessageCommandStep1{
		cmd2: &mockPublishMessageCommandStep2{
			cmd3: &mockPublishMessageCommandStep3{},
		},
	}

	return mc.cmd1
}

func (cmd1 *mockPublishMessageCommandStep1) MessageName(messageName string) commands.PublishMessageCommandStep2 {
	cmd1.messageName = messageName

	return cmd1.cmd2
}

func (cmd2 *mockPublishMessageCommandStep2) CorrelationKey(correlationKey string) commands.PublishMessageCommandStep3 {
	cmd2.correlationKey = correlationKey

	return cmd2.cmd3
}

// MessageId comes from the Zeebe client API and cannot be written as MessageID
// Note that when the `stylecheck` linter is working again, this method will need "nolink:stylecheck" (can't change name to ID or it won't satisfy an interface)
func (cmd3 *mockPublishMessageCommandStep3) MessageId(messageID string) commands.PublishMessageCommandStep3 {
	cmd3.messageID = messageID

	return cmd3
}

func (cmd3 *mockPublishMessageCommandStep3) TimeToLive(timeToLive time.Duration) commands.PublishMessageCommandStep3 {
	cmd3.timeToLive = timeToLive

	return cmd3
}

func (cmd3 *mockPublishMessageCommandStep3) VariablesFromObject(variables interface{}) (commands.PublishMessageCommandStep3, error) {
	cmd3.variables = variables

	return cmd3, nil
}

func (cmd3 *mockPublishMessageCommandStep3) Send(context.Context) (*pb.PublishMessageResponse, error) {
	return &pb.PublishMessageResponse{}, nil
}

func TestPublishMessage(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("messageName is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: PublishMessageOperation}
		_, err := cmd.Invoke(context.TODO(), req)
		assert.Error(t, err, ErrMissingMessageName)
	})

	t.Run("send message with mandatory fields", func(t *testing.T) {
		payload := publishMessagePayload{
			MessageName: "a",
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: PublishMessageOperation}

		var mc mockPublishMessageClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(context.TODO(), req)
		assert.NoError(t, err)

		assert.Equal(t, payload.MessageName, mc.cmd1.messageName)
		assert.Equal(t, payload.CorrelationKey, mc.cmd1.cmd2.correlationKey)
	})

	t.Run("send message with optional fields", func(t *testing.T) {
		payload := publishMessagePayload{
			MessageName:    "a",
			CorrelationKey: "b",
			MessageID:      "c",
			TimeToLive:     contrib_metadata.Duration{Duration: 1 * time.Second},
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: PublishMessageOperation}

		var mc mockPublishMessageClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(context.TODO(), req)
		assert.NoError(t, err)

		assert.Equal(t, payload.MessageName, mc.cmd1.messageName)
		assert.Equal(t, payload.CorrelationKey, mc.cmd1.cmd2.correlationKey)
		assert.Equal(t, payload.MessageID, mc.cmd1.cmd2.cmd3.messageID)
		assert.Equal(t, payload.TimeToLive.Duration, mc.cmd1.cmd2.cmd3.timeToLive)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.cmd3.variables)
	})
}
