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

type mockResolveIncident struct {
	zbc.Client
	cmd1 *mockResolveIncidentCommandStep1
}

type mockResolveIncidentCommandStep1 struct {
	commands.ResolveIncidentCommandStep1
	cmd2        *mockResolveIncidentCommandStep2
	incidentKey int64
}

type mockResolveIncidentCommandStep2 struct {
	commands.ResolveIncidentCommandStep2
}

func (mc *mockResolveIncident) NewResolveIncidentCommand() commands.ResolveIncidentCommandStep1 {
	mc.cmd1 = new(mockResolveIncidentCommandStep1)
	mc.cmd1.cmd2 = new(mockResolveIncidentCommandStep2)

	return mc.cmd1
}

func (cmd1 *mockResolveIncidentCommandStep1) IncidentKey(incidentKey int64) commands.ResolveIncidentCommandStep2 {
	cmd1.incidentKey = incidentKey

	return cmd1.cmd2
}

func (cmd2 *mockResolveIncidentCommandStep2) Send(context.Context) (*pb.ResolveIncidentResponse, error) {
	return &pb.ResolveIncidentResponse{}, nil
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

		mc := new(mockResolveIncident)

		message := ZeebeCommand{logger: testLogger, client: mc}
		_, err = message.Invoke(req)
		assert.Nil(t, err)

		assert.Equal(t, *payload.IncidentKey, mc.cmd1.incidentKey)
	})
}
