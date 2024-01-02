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

	"github.com/dapr/components-contrib/bindings"
	b_aws_s3 "github.com/dapr/components-contrib/bindings/aws/s3"
	b_azure_blobstorage "github.com/dapr/components-contrib/bindings/azure/blobstorage"
	b_azure_cosmosdb "github.com/dapr/components-contrib/bindings/azure/cosmosdb"
	b_azure_eventgrid "github.com/dapr/components-contrib/bindings/azure/eventgrid"
	b_azure_eventhubs "github.com/dapr/components-contrib/bindings/azure/eventhubs"
	b_azure_servicebusqueues "github.com/dapr/components-contrib/bindings/azure/servicebusqueues"
	b_azure_storagequeues "github.com/dapr/components-contrib/bindings/azure/storagequeues"
	b_cron "github.com/dapr/components-contrib/bindings/cron"
	b_http "github.com/dapr/components-contrib/bindings/http"
	b_influx "github.com/dapr/components-contrib/bindings/influx"
	b_kafka "github.com/dapr/components-contrib/bindings/kafka"
	b_kubemq "github.com/dapr/components-contrib/bindings/kubemq"
	b_mqtt3 "github.com/dapr/components-contrib/bindings/mqtt3"
	b_postgres "github.com/dapr/components-contrib/bindings/postgres"
	b_rabbitmq "github.com/dapr/components-contrib/bindings/rabbitmq"
	b_redis "github.com/dapr/components-contrib/bindings/redis"
	conf_bindings "github.com/dapr/components-contrib/tests/conformance/bindings"
)

func TestBindingsConformance(t *testing.T) {
	const configPath = "../config/bindings/"
	tc, err := NewTestConfiguration(filepath.Join(configPath, "tests.yml"))
	require.NoError(t, err)
	require.NotNil(t, tc)

	tc.TestFn = func(comp *TestComponent) func(t *testing.T) {
		return func(t *testing.T) {
			ParseConfigurationMap(t, comp.Config)

			componentConfigPath := convertComponentNameToPath(comp.Component, comp.Profile)
			props, err := loadComponentsAndProperties(t, filepath.Join(configPath, componentConfigPath))
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			inputBinding := loadInputBindings(comp.Component)
			outputBinding := loadOutputBindings(comp.Component)
			require.True(t, inputBinding != nil || outputBinding != nil)

			bindingsConfig, err := conf_bindings.NewTestConfig(comp.Component, comp.Operations, comp.Config)
			require.NoErrorf(t, err, "error running conformance test for component %s", comp.Component)

			conf_bindings.ConformanceTests(t, props, inputBinding, outputBinding, bindingsConfig)
		}
	}

	tc.Run(t)
}

func loadOutputBindings(name string) bindings.OutputBinding {
	switch name {
	case "redis.v6":
		return b_redis.NewRedis(testLogger)
	case "redis.v7":
		return b_redis.NewRedis(testLogger)
	case "azure.blobstorage":
		return b_azure_blobstorage.NewAzureBlobStorage(testLogger)
	case "azure.storagequeues":
		return b_azure_storagequeues.NewAzureStorageQueues(testLogger)
	case "azure.servicebusqueues":
		return b_azure_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "azure.eventgrid":
		return b_azure_eventgrid.NewAzureEventGrid(testLogger)
	case "azure.eventhubs":
		return b_azure_eventhubs.NewAzureEventHubs(testLogger)
	case "azure.cosmosdb":
		return b_azure_cosmosdb.NewCosmosDB(testLogger)
	case "kafka":
		return b_kafka.NewKafka(testLogger)
	case "http":
		return b_http.NewHTTP(testLogger)
	case "influx":
		return b_influx.NewInflux(testLogger)
	case "mqtt3":
		return b_mqtt3.NewMQTT(testLogger)
	case "rabbitmq":
		return b_rabbitmq.NewRabbitMQ(testLogger)
	case "kubemq":
		return b_kubemq.NewKubeMQ(testLogger)
	case "postgresql.docker":
		return b_postgres.NewPostgres(testLogger)
	case "postgresql.azure":
		return b_postgres.NewPostgres(testLogger)
	case "aws.s3.docker":
		return b_aws_s3.NewAWSS3(testLogger)
	case "aws.s3.terraform":
		return b_aws_s3.NewAWSS3(testLogger)
	default:
		return nil
	}
}

func loadInputBindings(name string) bindings.InputBinding {
	switch name {
	case "azure.servicebusqueues":
		return b_azure_servicebusqueues.NewAzureServiceBusQueues(testLogger)
	case "azure.storagequeues":
		return b_azure_storagequeues.NewAzureStorageQueues(testLogger)
	case "azure.eventgrid":
		return b_azure_eventgrid.NewAzureEventGrid(testLogger)
	case "cron":
		return b_cron.NewCron(testLogger)
	case "azure.eventhubs":
		return b_azure_eventhubs.NewAzureEventHubs(testLogger)
	case "kafka":
		return b_kafka.NewKafka(testLogger)
	case "mqtt3":
		return b_mqtt3.NewMQTT(testLogger)
	case "rabbitmq":
		return b_rabbitmq.NewRabbitMQ(testLogger)
	case "kubemq":
		return b_kubemq.NewKubeMQ(testLogger)
	default:
		return nil
	}
}
