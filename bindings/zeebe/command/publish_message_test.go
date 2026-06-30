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
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
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
func (cmd3 *mockPublishMessageCommandStep3) MessageId(messageID string) commands.PublishMessageCommandStep3 { //nolint:stylecheck
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

func TestPublishMessagePayloadUnmarshal(t *testing.T) {
	t.Run("invalid timeToLive produces field-scoped error", func(t *testing.T) {
		// ISO-8601 durations are not valid; Go duration strings like "1m30s" are required.
		raw := `{"messageName":"msg","timeToLive":"PT1M"}`
		var p publishMessagePayload
		err := p.UnmarshalJSON([]byte(raw))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeToLive")
		assert.Contains(t, err.Error(), "PT1M")
		assert.Contains(t, err.Error(), "duration string")
	})

	t.Run("unknown unit in timeToLive produces field-scoped error", func(t *testing.T) {
		raw := `{"messageName":"msg","timeToLive":"1minute"}`
		var p publishMessagePayload
		err := p.UnmarshalJSON([]byte(raw))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeToLive")
		assert.Contains(t, err.Error(), "1minute")
		assert.Contains(t, err.Error(), "duration string")
	})

	t.Run("valid Go duration string succeeds", func(t *testing.T) {
		raw := `{"messageName":"msg","timeToLive":"1m30s"}`
		var p publishMessagePayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		require.NotNil(t, p.TimeToLive)
		assert.Equal(t, 90*time.Second, p.TimeToLive.Duration)
	})

	t.Run("numeric nanosecond timeToLive still parses correctly", func(t *testing.T) {
		// Regression guard: numeric (nanosecond) values were accepted before and must remain valid.
		raw := `{"messageName":"msg","timeToLive":30000000000}`
		var p publishMessagePayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		require.NotNil(t, p.TimeToLive)
		assert.Equal(t, 30*time.Second, p.TimeToLive.Duration)
	})

	t.Run("null timeToLive leaves pointer nil", func(t *testing.T) {
		raw := `{"messageName":"msg","timeToLive":null}`
		var p publishMessagePayload
		err := p.UnmarshalJSON([]byte(raw))
		require.NoError(t, err)
		assert.Nil(t, p.TimeToLive)
	})
}

func TestPublishMessage(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("messageName is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		payload := map[string]string{}
		data, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		req := &bindings.InvokeRequest{Operation: PublishMessageOperation, Data: data}
		_, err := cmd.Invoke(t.Context(), req)
		require.ErrorIs(t, err, ErrMissingMessageName)
	})

	t.Run("send message with mandatory fields", func(t *testing.T) {
		payload := publishMessagePayload{
			MessageName: "a",
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: PublishMessageOperation}

		var mc mockPublishMessageClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(t.Context(), req)
		require.NoError(t, err)

		assert.Equal(t, payload.MessageName, mc.cmd1.messageName)
		assert.Equal(t, payload.CorrelationKey, mc.cmd1.cmd2.correlationKey)
	})

	t.Run("send message with optional fields", func(t *testing.T) {
		payload := publishMessagePayload{
			MessageName:    "a",
			CorrelationKey: "b",
			MessageID:      "c",
			TimeToLive:     &kitmd.Duration{Duration: 1 * time.Second},
			Variables: map[string]interface{}{
				"key": "value",
			},
		}
		data, err := json.Marshal(payload)
		require.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: PublishMessageOperation}

		var mc mockPublishMessageClient

		cmd := ZeebeCommand{logger: testLogger, client: &mc}
		_, err = cmd.Invoke(t.Context(), req)
		require.NoError(t, err)

		assert.Equal(t, payload.MessageName, mc.cmd1.messageName)
		assert.Equal(t, payload.CorrelationKey, mc.cmd1.cmd2.correlationKey)
		assert.Equal(t, payload.MessageID, mc.cmd1.cmd2.cmd3.messageID)
		assert.Equal(t, payload.TimeToLive.Duration, mc.cmd1.cmd2.cmd3.timeToLive)
		assert.Equal(t, payload.Variables, mc.cmd1.cmd2.cmd3.variables)
	})
}
