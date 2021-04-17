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
	"github.com/stretchr/testify/mock"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
)

type MockPublishMessageCommandStep1 struct {
	mock.Mock
}

type MockPublishMessageCommandStep2 struct {
	mock.Mock
}

type MockPublishMessageCommandStep3 struct {
	commands.PublishMessageCommandStep3
	mock.Mock
}

func (mc *MockClient) NewPublishMessageCommand() commands.PublishMessageCommandStep1 {
	return mc.Called().Get(0).(commands.PublishMessageCommandStep1)
}

func (cmd1 *MockPublishMessageCommandStep1) MessageName(messageName string) commands.PublishMessageCommandStep2 {
	return cmd1.Called(messageName).Get(0).(commands.PublishMessageCommandStep2)
}

func (cmd2 *MockPublishMessageCommandStep2) CorrelationKey(correlationKey string) commands.PublishMessageCommandStep3 {
	return cmd2.Called(correlationKey).Get(0).(commands.PublishMessageCommandStep3)
}

//nolint // MessageId comes from the Zeebe client API and cannot be written as MessageID
func (cmd3 *MockPublishMessageCommandStep3) MessageId(messageID string) commands.PublishMessageCommandStep3 {
	return cmd3.Called(messageID).Get(0).(commands.PublishMessageCommandStep3)
}

func (cmd3 *MockPublishMessageCommandStep3) TimeToLive(duration time.Duration) commands.PublishMessageCommandStep3 {
	return cmd3.Called(duration).Get(0).(commands.PublishMessageCommandStep3)
}

func (cmd3 *MockPublishMessageCommandStep3) VariablesFromObject(variables interface{}) (commands.PublishMessageCommandStep3, error) {
	args := cmd3.Called(variables)

	return args.Get(0).(commands.PublishMessageCommandStep3), args.Error(1)
}

func (cmd3 *MockPublishMessageCommandStep3) Send(context context.Context) (*pb.PublishMessageResponse, error) {
	args := cmd3.Called(context)

	return args.Get(0).(*pb.PublishMessageResponse), args.Error(1)
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

		mc := new(MockClient)
		cmd1 := new(MockPublishMessageCommandStep1)
		cmd2 := new(MockPublishMessageCommandStep2)
		cmd3 := new(MockPublishMessageCommandStep3)

		mc.On("NewPublishMessageCommand").Return(cmd1)
		cmd1.On("MessageName", payload.MessageName).Return(cmd2)
		cmd2.On("CorrelationKey", payload.CorrelationKey).Return(cmd3)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.PublishMessageResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
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

		mc := new(MockClient)
		cmd1 := new(MockPublishMessageCommandStep1)
		cmd2 := new(MockPublishMessageCommandStep2)
		cmd3 := new(MockPublishMessageCommandStep3)

		mc.On("NewPublishMessageCommand").Return(cmd1)
		cmd1.On("MessageName", payload.MessageName).Return(cmd2)
		cmd2.On("CorrelationKey", payload.CorrelationKey).Return(cmd3)
		cmd3.On("MessageId", payload.MessageID).Return(cmd3)
		cmd3.On("TimeToLive", payload.TimeToLive.Duration).Return(cmd3)
		cmd3.On("VariablesFromObject", payload.Variables).Return(cmd3, nil)
		cmd3.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.PublishMessageResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
		cmd3.AssertExpectations(t)
	})
}
