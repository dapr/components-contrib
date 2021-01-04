// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zeebe-io/zeebe/clients/go/pkg/zbc"
)

type MockClientFactory struct {
	mock.Mock
}

type MockClient struct {
	zbc.Client
	mock.Mock
}

func (mcf *MockClientFactory) Get(metadata bindings.Metadata) (zbc.Client, error) {
	args := mcf.Called(metadata)

	return args.Get(0).(zbc.Client), args.Error(1)
}

func TestInit(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("returns error if client could not be instantiated properly", func(t *testing.T) {
		errorMsg := "error on parsing metadata"
		metadata := bindings.Metadata{}
		mcf := new(MockClientFactory)
		mc := new(MockClient)

		mcf.On("Get", metadata).Return(mc, errors.New(errorMsg))

		message := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := message.Init(metadata)
		assert.Error(t, err, errorMsg)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		mcf := new(MockClientFactory)
		mc := new(MockClient)

		mcf.On("Get", metadata).Return(mc, nil)

		message := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := message.Init(metadata)

		assert.Nil(t, err)
		assert.Equal(t, mc, message.client)

		mcf.AssertExpectations(t)
		mc.AssertExpectations(t)
	})
}

func TestInvoke(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("operation must be supported", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: bindings.DeleteOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, fmt.Sprintf(unsupportedOperationErrorMsg, bindings.DeleteOperation))
	})
}

func TestOperations(t *testing.T) {
	testBinding := ZeebeCommand{logger: logger.NewLogger("test")}
	operations := testBinding.Operations()
	assert.Equal(t, 12, len(operations))
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
