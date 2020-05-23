# Pub Sub

Pub Subs provide a common way to interact with different message bus implementations to achieve reliable, high-scale scenarios based on event-driven async communications, while allowing users to opt-in to advanced capabilities using defined metadata.

Currently supported pub-subs are:

* Hazelcast
* Redis Streams
* NATS
* Kafka
* Azure Service Bus
* RabbitMQ
* Azure Event Hubs
* GCP Pub/Sub
* MQTT

## Implementing a new Pub Sub

A compliant pub sub needs to implement the following interface:

```go
type PubSub interface {
	Init(metadata Metadata) error
	Publish(req *PublishRequest) error
	Subscribe(req SubscribeRequest, handler func(msg *NewMessage) error) error
}
```
