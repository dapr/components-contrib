//go:build conftests
// +build conftests

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

package conformance

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
	p_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	p_eventhubs "github.com/dapr/components-contrib/pubsub/azure/eventhubs"
	p_servicebusqueues "github.com/dapr/components-contrib/pubsub/azure/servicebus/queues"
	p_servicebustopics "github.com/dapr/components-contrib/pubsub/azure/servicebus/topics"
	p_gcppubsub "github.com/dapr/components-contrib/pubsub/gcp/pubsub"
	p_inmemory "github.com/dapr/components-contrib/pubsub/in-memory"
	p_jetstream "github.com/dapr/components-contrib/pubsub/jetstream"
	p_kafka "github.com/dapr/components-contrib/pubsub/kafka"
	p_kubemq "github.com/dapr/components-contrib/pubsub/kubemq"
	p_mqtt3 "github.com/dapr/components-contrib/pubsub/mqtt3"
	p_pulsar "github.com/dapr/components-contrib/pubsub/pulsar"
	p_rabbitmq "github.com/dapr/components-contrib/pubsub/rabbitmq"
	p_redis "github.com/dapr/components-contrib/pubsub/redis"
	p_solaceamqp "github.com/dapr/components-contrib/pubsub/solace/amqp"
	conf_pubsub "github.com/dapr/components-contrib/tests/conformance/pubsub"
)

func TestPubsubConformance(t *testing.T) {
	const configPath = "../config/pubsub/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			pubsub := loadPubSub(comp.Component)
			require.NotNil(t, pubsub, "error running conformance test for component %s", comp.Component)

			pubsubConfig, err := conf_pubsub.NewTestConfig(comp.Component, comp.Operations, comp.Config)
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conf_pubsub.ConformanceTests(t, props, pubsub, pubsubConfig)
		}
	}

	tc.Run(t)
}

func loadPubSub(name string) pubsub.PubSub {
	switch name {
	case "redis.v6":
		return p_redis.NewRedisStreams(testLogger)
	case "redis.v7":
		return p_redis.NewRedisStreams(testLogger)
	case "azure.eventhubs":
		return p_eventhubs.NewAzureEventHubs(testLogger)
	case "azure.servicebus.topics":
		return p_servicebustopics.NewAzureServiceBusTopics(testLogger)
	case "azure.servicebus.queues":
		return p_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "jetstream":
		return p_jetstream.NewJetStream(testLogger)
	case "kafka":
		return p_kafka.NewKafka(testLogger)
	case "pulsar":
		return p_pulsar.NewPulsar(testLogger)
	case "mqtt3":
		return p_mqtt3.NewMQTTPubSub(testLogger)
	case "rabbitmq":
		return p_rabbitmq.NewRabbitMQ(testLogger)
	case "in-memory":
		return p_inmemory.New(testLogger)
	case "aws.snssqs.terraform":
		return p_snssqs.NewSnsSqs(testLogger)
	case "aws.snssqs.docker":
		return p_snssqs.NewSnsSqs(testLogger)
	case "gcp.pubsub.terraform":
		return p_gcppubsub.NewGCPPubSub(testLogger)
	case "gcp.pubsub.docker":
		return p_gcppubsub.NewGCPPubSub(testLogger)
	case "kubemq":
		return p_kubemq.NewKubeMQ(testLogger)
	case "solace.amqp":
		return p_solaceamqp.NewAMQPPubsub(testLogger)
	default:
		return nil
	}
}
