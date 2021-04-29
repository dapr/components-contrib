// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/kit/logger"
)

type mockClientFactory struct {
	zeebe.ClientFactory
	metadata bindings.Metadata
	error    error
}

type mockClient struct {
	zbc.Client
}

func (mcf mockClientFactory) Get(metadata bindings.Metadata) (zbc.Client, error) {
	mcf.metadata = metadata

	if mcf.error != nil {
		return nil, mcf.error
	}

	return mockClient{}, nil
}

func TestInit(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("returns error if client could not be instantiated properly", func(t *testing.T) {
		errParsing := errors.New("error on parsing metadata")
		metadata := bindings.Metadata{}
		mcf := mockClientFactory{
			error: errParsing,
		}

		command := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := command.Init(metadata)
		assert.Error(t, err, errParsing)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		mcf := mockClientFactory{}

		command := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := command.Init(metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, command.client)
		assert.Equal(t, metadata, mcf.metadata)
	})
}

func TestInvoke(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("operation must be supported", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: bindings.DeleteOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrUnsupportedOperation(bindings.DeleteOperation))
	})
}

func TestOperations(t *testing.T) {
	testBinding := ZeebeCommand{logger: logger.NewLogger("test")}
	operations := testBinding.Operations()
	require.Equal(t, 12, len(operations))
	assert.Equal(t, topologyOperation, operations[0])
	assert.Equal(t, deployWorkflowOperation, operations[1])
	assert.Equal(t, createInstanceOperation, operations[2])
	assert.Equal(t, cancelInstanceOperation, operations[3])
	assert.Equal(t, setVariablesOperation, operations[4])
	assert.Equal(t, resolveIncidentOperation, operations[5])
	assert.Equal(t, publishMessageOperation, operations[6])
	assert.Equal(t, activateJobsOperation, operations[7])
	assert.Equal(t, completeJobOperation, operations[8])
	assert.Equal(t, failJobOperation, operations[9])
	assert.Equal(t, updateJobRetriesOperation, operations[10])
	assert.Equal(t, throwErrorOperation, operations[11])
}
