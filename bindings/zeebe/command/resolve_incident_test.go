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
	"github.com/stretchr/testify/mock"
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
	"github.com/zeebe-io/zeebe/clients/go/pkg/pb"
)

type MockResolveIncidentCommandStep1 struct {
	mock.Mock
}

type MockResolveIncidentCommandStep2 struct {
	mock.Mock
}

func (mc *MockClient) NewResolveIncidentCommand() commands.ResolveIncidentCommandStep1 {
	return mc.Called().Get(0).(commands.ResolveIncidentCommandStep1)
}

func (cmd1 *MockResolveIncidentCommandStep1) IncidentKey(incidentKey int64) commands.ResolveIncidentCommandStep2 {
	return cmd1.Called(incidentKey).Get(0).(commands.ResolveIncidentCommandStep2)
}

func (cmd2 *MockResolveIncidentCommandStep2) Send(context context.Context) (*pb.ResolveIncidentResponse, error) {
	args := cmd2.Called(context)

	return args.Get(0).(*pb.ResolveIncidentResponse), args.Error(1)
}

func TestResolveIncident(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("incidentKey is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: resolveIncidentOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingIncidentKey)
	})

	t.Run("resolve a incident", func(t *testing.T) {
		payload := resolveIncidentPayload{
			IncidentKey: new(int64),
		}
		data, err := json.Marshal(payload)
		assert.Nil(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: resolveIncidentOperation}

		mc := new(MockClient)
		cmd1 := new(MockResolveIncidentCommandStep1)
		cmd2 := new(MockResolveIncidentCommandStep2)

		mc.On("NewResolveIncidentCommand").Return(cmd1)
		cmd1.On("IncidentKey", *payload.IncidentKey).Return(cmd2)
		cmd2.On("Send", mock.AnythingOfType("*context.emptyCtx")).Return(new(pb.ResolveIncidentResponse), nil)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		mc.AssertExpectations(t)
		cmd1.AssertExpectations(t)
		cmd2.AssertExpectations(t)
	})
}
