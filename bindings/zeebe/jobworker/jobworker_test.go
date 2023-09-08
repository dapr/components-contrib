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

package jobworker

import (
	"context"
	"errors"
	"testing"

	"github.com/camunda/zeebe/clients/go/v8/pkg/zbc"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe"
	"github.com/dapr/components-contrib/metadata"
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

func (mcf *mockClientFactory) Get(metadata bindings.Metadata) (zbc.Client, error) {
	mcf.metadata = metadata

	if mcf.error != nil {
		return nil, mcf.error
	}

	return &mockClient{}, nil
}

func (mc *mockClient) Close() error {
	return nil
}

func TestInit(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("jobType is mandatory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		var mcf mockClientFactory

		jobWorker := ZeebeJobWorker{clientFactory: &mcf, logger: testLogger, closeCh: make(chan struct{})}
		err := jobWorker.Init(context.Background(), metadata)

		assert.Error(t, err, ErrMissingJobType)
		assert.NoError(t, jobWorker.Close())
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{Base: metadata.Base{
			Properties: map[string]string{"jobType": "a"},
		}}
		mcf := &mockClientFactory{
			metadata: metadata,
		}
		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger, closeCh: make(chan struct{})}
		err := jobWorker.Init(context.Background(), metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, jobWorker.client)
		assert.Equal(t, metadata, mcf.metadata)
		assert.NoError(t, jobWorker.Close())
	})

	t.Run("returns error if client could not be instantiated properly", func(t *testing.T) {
		errParsing := errors.New("error on parsing metadata")
		metadata := bindings.Metadata{}
		mcf := &mockClientFactory{
			error: errParsing,
		}

		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger, closeCh: make(chan struct{})}
		err := jobWorker.Init(context.Background(), metadata)
		assert.Error(t, err, errParsing)
		assert.NoError(t, jobWorker.Close())
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{Base: metadata.Base{
			Properties: map[string]string{"jobType": "a"},
		}}
		mcf := &mockClientFactory{
			metadata: metadata,
		}

		jobWorker := ZeebeJobWorker{clientFactory: mcf, logger: testLogger, closeCh: make(chan struct{})}
		err := jobWorker.Init(context.Background(), metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, jobWorker.client)
		assert.Equal(t, metadata, mcf.metadata)
		assert.NoError(t, jobWorker.Close())
	})
}
