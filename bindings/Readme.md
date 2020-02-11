# Bindings

Bindings provide a common way to trigger an application with events from external systems, or invoke an external system with optional data payloads.
Bindings are great for event-driven, on-demand compute and help reduce boilerplate code.

For detailed binding specs visit [Dapr docs](https://github.com/dapr/docs/tree/master/concepts/bindings/specs).

List of bindings and their status:

## Generic

| Name | Input<br>Binding | Output<br>Binding | Spec | Status |
|------|:----------------:|:-----------------:|------|--------|
| [HTTP](./http) |  | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/http.md) | Experimental |
| [Kafka](./kafka) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/kafka.md) | Experimental |
| [Kubernetes Events](./kubernetes) | ✅| | [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/kubernetes.md) | Experimental |
| [MQTT](./mqtt) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/mqtt.md) | Experimental |
| [RabbitMQ](./rabbitmq) | ✅ | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/rabbitmq.md) | Experimental |
| [Redis](./redis) |  | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/redis.md) | Experimental |
| [Twilio](./twilio) | | ✅ | [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/twilio.md) | Experimental |

## Amazon Web Service (AWS)

| Name | Input<br>Binding | Output<br>Binding | Spec | Status |
|------|:----------------:|:-----------------:|------|--------|
| [AWS DynamoDB](./aws/dynamodb) | | ✅ | [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/dynamodb.md) | Experimental |
| [AWS S3](./aws/s3) | | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/s3.md) | Experimental |
| [AWS SNS](./aws/sns) |  | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/sns.md) | Experimental |
| [AWS SQS](./aws/sqs) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/sqs.md) | Experimental |

## Google Cloud Platform (GCP)

| Name | Input<br>Binding | Output<br>Binding | Spec | Status |
|------|:----------------:|:-----------------:|------|--------|
| [GCP Cloud Pub/Sub](./gcp/pubsub) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/gcppubsub.md) | Experimental |
| [GCP Storage Bucket](./gcp/bucket)  | | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/gcpbucket.md) | Experimental |

## Microsoft Azure

| Name | Input<br>Binding | Output<br>Binding | Spec | Status |
|------|:----------------:|:-----------------:|------|--------|
| [Azure Blob Storage](./azure/blobstorage) | | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/blobstorage.md) | Experimental |
| [Azure EventHubs](./azure/eventhubs) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/eventhubs.md) | Experimental |
| [Azure CosmosDB](./azure/cosmosdb) | | ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/cosmosdb.md) | Experimental |
| [Azure Service Bus Queues](./azure/servicebusqueues) | ✅| ✅| [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/servicebusqueues.md) | Experimental |
| [Azure SignalR](./azure/signalr) | | ✅ | [Spec](https://github.com/dapr/docs/blob/master/concepts/bindings/specs/signalr.md) | Experimental |

## Implementing a new binding

A compliant binding needs to implement one or more interfaces, depending on the type of binding (Input or Output):

Input binding:

```
type InputBinding interface {
	Init(metadata Metadata) error
	Read(handler func(*ReadResponse) error) error
}
```

Output binding:

```
type OutputBinding interface {
	Init(metadata Metadata) error
	Write(req *WriteRequest) error
}
```
