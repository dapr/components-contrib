/*
Copyright 2023 The Dapr Authors
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

package command_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
)

func TestTopologyOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	testCheckTopologyData := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		res, _ := zeebe_test.ExecCommandOperation(
			ctx,
			client,
			bindings_zeebe_command.TopologyOperation,
			nil,
			map[string]string{})

		envVars := zeebe_test.GetEnvVars("../.env")
		topology := &pb.TopologyResponse{}
		err := json.Unmarshal(res.Data, topology)
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

		return nil
	}

	flow.New(t, "Test topology operation").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Check topology data", testCheckTopologyData).
		Run()
}
