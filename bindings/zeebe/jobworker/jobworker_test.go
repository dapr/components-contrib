// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package jobworker

import (
	"errors"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"
	"github.com/stretchr/testify/assert"

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

	t.Run("jobType is mandatory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		var mcf mockClientFactory

		jobWorker := ZeebeJobWorker{clientFactory: &mcf, logger: testLogger}
		err := jobWorker.Init(metadata)

		assert.Error(t, err, ErrMissingJobType)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{"jobType": "a"},
		}
		mcf := mockClientFactory{
			metadata: metadata,
		}
		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger}
		err := jobWorker.Init(metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, jobWorker.client)
		assert.Equal(t, metadata, mcf.metadata)
	})

	t.Run("returns error if client could not be instantiated properly", func(t *testing.T) {
		errParsing := errors.New("error on parsing metadata")
		metadata := bindings.Metadata{}
		mcf := mockClientFactory{
			error: errParsing,
		}

		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger}
		err := jobWorker.Init(metadata)
		assert.Error(t, err, errParsing)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{"jobType": "a"},
		}
		mcf := mockClientFactory{
			metadata: metadata,
		}

		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger}
		err := jobWorker.Init(metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, jobWorker.client)
		assert.Equal(t, metadata, mcf.metadata)
	})
}
