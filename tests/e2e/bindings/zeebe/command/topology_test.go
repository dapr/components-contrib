//go:build e2etests
// +build e2etests

/*
Copyright 2022 The Dapr Authors
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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestTopology(t *testing.T) {
	t.Parallel()

	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	envVars := zeebe.GetEnvVars()

	t.Run("get the topology data", func(t *testing.T) {
		t.Parallel()

		req := &bindings.InvokeRequest{Operation: command.TopologyOperation}
		res, err := cmd.Invoke(req)
		assert.NoError(t, err)

		topology := &pb.TopologyResponse{}
		err = json.Unmarshal(res.Data, topology)
		assert.NoError(t, err)

		expectedClusterSize, err := strconv.ParseInt(envVars.ZeebeBrokerClusterSize, 10, 32)
		assert.NoError(t, err)

		expectedPartitionsCount, err := strconv.ParseInt(envVars.ZeebeBrokerPartitionsCount, 10, 32)
		assert.NoError(t, err)

		expectedReplicationFactor, err := strconv.ParseInt(envVars.ZeebeBrokerReplicationFactor, 10, 32)
		assert.NoError(t, err)

		brokerGatewayPort, err := strconv.ParseInt(envVars.ZeebeBrokerGatewayPort, 10, 32)
		assert.NoError(t, err)

		expectedBrokerPort := brokerGatewayPort + 1

		assert.NoError(t, err)
		assert.Nil(t, res.Metadata)
		assert.Equal(t, 1, len(topology.Brokers))
		assert.Equal(t, int32(0), topology.Brokers[0].NodeId)
		assert.Equal(t, envVars.ZeebeBrokerHost, topology.Brokers[0].Host)
		assert.Equal(t, int32(expectedBrokerPort), topology.Brokers[0].Port)
		assert.Equal(t, envVars.ZeebeVersion, topology.Brokers[0].Version)
		assert.Equal(t, int(expectedPartitionsCount), len(topology.Brokers[0].Partitions))
		assert.Equal(t, int32(expectedPartitionsCount), topology.Brokers[0].Partitions[0].PartitionId)
		assert.Equal(t, pb.Partition_PartitionBrokerRole(0), topology.Brokers[0].Partitions[0].Role)
		assert.Equal(t, pb.Partition_PartitionBrokerHealth(0), topology.Brokers[0].Partitions[0].Health)
		assert.Equal(t, int32(expectedClusterSize), topology.ClusterSize)
		assert.Equal(t, int32(expectedPartitionsCount), topology.PartitionsCount)
		assert.Equal(t, int32(expectedReplicationFactor), topology.ReplicationFactor)
		assert.Equal(t, envVars.ZeebeVersion, topology.GatewayVersion)
	})
}
