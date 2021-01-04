// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package jobworker

import (
	"errors"
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

		message := ZeebeJobWorker{clientFactory: mcf, logger: testLogger}
		err := message.Init(metadata)
		assert.Error(t, err, errorMsg)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		mcf := new(MockClientFactory)
		mc := new(MockClient)

		mcf.On("Get", metadata).Return(mc, nil)

		message := ZeebeJobWorker{clientFactory: mcf, logger: testLogger}
		err := message.Init(metadata)

		assert.Nil(t, err)
		assert.Equal(t, mc, message.client)

		mcf.AssertExpectations(t)
		mc.AssertExpectations(t)
	})
}
