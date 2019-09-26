# Bindings

Bindings provide a common way to trigger an application with events from external systems, or invoke an external system with optional data payloads.
Bindings are great for event-driven, on-demand compute and help reduce boilerplate code.

Currently supported bindings are:

* Kafka
* RabbitMQ
* AWS SQS
* AWS SNS
* Azure Eventhubs
* Azure CosmosDB
* Google Cloud Storage Bucket
* HTTP
* MQTT
* AWS DynamoDB
* Redis

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
