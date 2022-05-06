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
	"errors"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/zbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

		cmd := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := cmd.Init(metadata)
		assert.Error(t, err, errParsing)
	})

	t.Run("sets client from client factory", func(t *testing.T) {
		metadata := bindings.Metadata{}
		mcf := mockClientFactory{}

		cmd := ZeebeCommand{clientFactory: mcf, logger: testLogger}
		err := cmd.Init(metadata)

		assert.NoError(t, err)

		mc, err := mcf.Get(metadata)

		assert.NoError(t, err)
		assert.Equal(t, mc, cmd.client)
		assert.Equal(t, metadata, mcf.metadata)
	})
}

func TestInvoke(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("operation must be supported", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: bindings.DeleteOperation}
		_, err := cmd.Invoke(context.TODO(), req)
		assert.Error(t, err, ErrUnsupportedOperation(bindings.DeleteOperation))
	})
}

func TestOperations(t *testing.T) {
	testBinding := ZeebeCommand{logger: logger.NewLogger("test")}
	operations := testBinding.Operations()
	require.Equal(t, 12, len(operations))
	assert.Equal(t, TopologyOperation, operations[0])
	assert.Equal(t, DeployProcessOperation, operations[1])
	assert.Equal(t, CreateInstanceOperation, operations[2])
	assert.Equal(t, CancelInstanceOperation, operations[3])
	assert.Equal(t, SetVariablesOperation, operations[4])
	assert.Equal(t, ResolveIncidentOperation, operations[5])
	assert.Equal(t, PublishMessageOperation, operations[6])
	assert.Equal(t, ActivateJobsOperation, operations[7])
	assert.Equal(t, CompleteJobOperation, operations[8])
	assert.Equal(t, FailJobOperation, operations[9])
	assert.Equal(t, UpdateJobRetriesOperation, operations[10])
	assert.Equal(t, ThrowErrorOperation, operations[11])
}
