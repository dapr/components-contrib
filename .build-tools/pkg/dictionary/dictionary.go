/* Copyright 2024 The Dapr Authors
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

package dictionary

const (
	binding       = "bindings"
	state         = "state"
	configuration = "configuration"
	conversation  = "conversation"
	middleware    = "middleware"
	pubsub        = "pubsub"
	secretstores  = "secretstores"
)

const (
	// state
	nameSqlserver         = "sqlserver"
	nameTableStorage      = "azure.tablestorage"
	nameCassandra         = "cassandra"
	nameCloudflareWorkers = "cloudflare.workerskv"
	nameCockroach         = "cockroachdb"
	nameMemcached         = "memcached"
	nameMongo             = "mongodb"

	// bindings
	nameS3            = "aws.s3"
	nameSns           = "aws.sns"
	nameCosmosSql     = "azure.cosmosdb"
	nameCosmosGremlin = "azure.cosmosdb.gremlinapi"
	nameEventGrid     = "azure.eventgrid"
	nameEventHub      = "azure.eventhubs"
	nameOpenAI        = "azure.openai"
	nameStorageQueues = "azure.storagequeues"
	nameSignalR       = "azure.signalr"
	nameCron          = "cron"
	nameGCPBucket     = "gcp.bucket"
	nameHTTP          = "http"
	nameMySQL         = "mysql"
	nameRabbit        = "rabbitmq"
	nameZeebeCommand  = "zeebe.command"
	nameZeebeWorker   = "zeebe.jobworker"
	nameAppConfig     = "azure.appconfig"
	nameKinesis       = "aws.kinesis"

	// configuration

	// conversation
	nameAnthropic = "anthropic"
	nameBedrock   = "aws.bedrock"
	nameHugging   = "huggingface"
	nameMistral   = "mistral"

	// middleware
	nameRouter = "routeralias"

	// pubsub
	nameSnsSqs           = "aws.snssqs"
	nameServiceBusTopics = "azure.servicebus.topics"
	nameGCPPubsub        = "gcp.pubsub"
	nameMQTT             = "mqtt3"
	namePulsar           = "pulsar"
	nameAmqp             = "solace.amqp"

	// secretstores
	nameParameterStore = "aws.parameterstore"
	nameSecretManager  = "aws.secretsmanager"
	nameAzureVault     = "azure.keyvault"
	nameGCPSecret      = "gcp.secretmanager"
	nameHashiVault     = "hashicorp.vault"
	nameK8s            = "kubernetes"
	nameLocal          = "local.env"

	// multiple component types
	nameRedis            = "redis"
	namePostgresql       = "postgresql"
	nameKafka            = "kafka"
	nameServiceBusQueues = "azure.servicebus.queues"
	nameDynamo           = "aws.dynamodb"
	nameBlob             = "azure.blobstorage"
	nameInMem            = "in-memory"
)

const (
	v1 = "v1"
	v2 = "v2"
)

const (
	alpha  = "alpha"
	beta   = "beta"
	stable = "stable"
)

const (
	// state
	titleSqlserver         = "SQL Server"
	titleTableStorage      = "Azure Table Storage"
	titleCassandra         = "Apache Cassandra"
	titleCloudflareWorkers = "Cloudflare Workers KV"
	titleCockroach         = "CockroachDB"
	titleMemcached         = "Memcached"
	titleMongo             = "MongoDB"

	// bindings
	titleS3            = "AWS S3"
	titleSns           = "AWS SNS"
	titleCosmosSql     = "Azure Cosmos DB (SQL API)"
	titleCosmosGremlim = "Azure Cosmos DB (Gremlin API)"
	titleEventGrid     = "Azure Event Grid"
	titleEventHub      = "Azure Event Hubs"
	titleOpenAI        = "Azure OpenAI"
	titleStorageQueues = "Azure Storage Queues"
	titleSignalR       = "Azure SignalR"
	titleCron          = "Cron"
	titleGCPBucket     = "GCP Storage Bucket"
	titleHTTP          = "HTTP"
	titleMySQL         = "MySQL & MariaDB"
	titleRabbit        = "RabbitMQ"
	titleZeebeCommand  = "Zeebe Command"
	titleZeebeWorker   = "Zeebe JobWorker"
	titleAppConfig     = "Azure App Configuration"
	titleKinesis       = "AWS Kinesis Data Streams"

	// configuration

	// conversation
	titleAnthropic = "Anthropic"
	titleBedrock   = "AWS Bedrock"
	titleHugging   = "Huggingface"
	titleMistral   = "Mistral"

	// middleware
	titleRouter = "Router Alias"

	// pubsub
	titleSnsSqs           = "AWS SNS/SQS"
	titleServiceBusTopics = "Azure Service Bus Topics"
	titleGCPPubsub        = "GCP Pub/Sub"
	titleMQTT             = "MQTT3"
	titlePulsar           = "Apache Pulsar"
	titleAmqp             = "Solace-AMQP"

	// secretstores
	titleParameterStore = "AWS SSM Parameter Store"
	titleSecretManager  = "AWS Secrets manager"
	titleAzureVault     = "Azure Key Vault"
	titleGCPSecret      = "GCP Secret Manager"
	titleHashiVault     = "Hashicorp Vault"
	titleK8s            = "Kubernetes"
	titleLocal          = "Local environment variables"

	// multiple component types
	titlePostgresql       = "PostgreSQL"
	titleRedis            = "Redis"
	titleServiceBusQueues = "Azure Service Bus Queues"
	titleKafka            = "Apache Kafka"
	titleDynamo           = "AWS DynamoDB"
	titleBlob             = "Azure Blob Storage"
	titleInMem            = "In-memory"
)

const (
	descriptionSqlserver = "Microsoft SQL Server and Azure SQL"
)

const (
	capabilitiesEtag        = "etag"
	capabilitiesTtl         = "ttl"
	capabilitiesTransaction = "transaction"
	capabilitiesActorState  = "actorStateStore"
	capabilitiesCrud        = "crud"
	capabilitiesQuery       = "query"
)
