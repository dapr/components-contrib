// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
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
	mc.cmd1 = new(mockPublishMessageCommandStep1)
	mc.cmd1.cmd2 = new(mockPublishMessageCommandStep2)
	mc.cmd1.cmd2.cmd3 = new(mockPublishMessageCommandStep3)

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

//nolint // MessageId comes from the Zeebe client API and cannot be written as MessageID
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
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: publishMessageOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingMessageName)
	})

	t.Run("send message with mandatory fields", func(t *testing.T) {
		payload := publishMessagePayload{
			MessageName: "a",
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: publishMessageOperation}

		var mc mockPublishMessageClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

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
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: publishMessageOperation}

		var mc mockPublishMessageClient

		message := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		assert.Equal(t, payload.MessageName, mc.cmd1.messageName)
		assert.Equal(t, payload.CorrelationKey, mc.cmd1.cmd2.correlationKey)
		assert.Equal(t, payload.MessageID, mc.cmd1.cmd2.cmd3.messageID)
		assert.Equal(t, payload.TimeToLive.Duration, mc.cmd1.cmd2.cmd3.timeToLive)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.cmd3.variables)
	})
}
