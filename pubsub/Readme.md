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

### TTL (or Time To Live)

Time to live is implemented by default in Dapr. A publishing application can add the `expiration` attribute to a CloudEvent object and Dapr runtime will drop the message if the expiration is due at the subscriber's end, prior to invoking subscriber applications. The `expiration` attribute is Dapr specific and extends the CloudEvent spec. This is a convenience to subscriber applications, so Dapr does not expire messages from pub subs that do not have subscribers via Dapr.

Alternatively, the publisher application can set the expiration by publishing with the `ttlInSeconds` metadata. Components that support TTL should parse this metadata attribute. Dapr runtime will also automatically populate the `expiration` attribute in the CloudEvent object if `ttlInSeconds` is present - in this case, Dapr will add 1 second to allow component implementations to expire the message before Dapr consumes it.

As per the CloudEvent spec, `expiration` is formatted using RFC3339.

Example:
```json
{
	"specversion" : "1.0",
	"type" : "com.github.pull.create",
	"source" : "https://github.com/cloudevents/spec/pull",
	"subject" : "123",
	"id" : "A234-1234-1234",
	"expiration" : "2018-04-06T17:31:00Z",
	"datacontenttype" : "text/xml",
	"data" : "<much wow=\"xml\"/>"
}
```

If the pub sub component implementation can handle TTL natively without relying on Dapr, consume the `ttlInSeconds` metadata in the component implementation for the Publish function.

Example:
```go
import contrib_metadata "github.com/dapr/components-contrib/metadata"

//...

func (c *MyComponent) Publish(req *pubsub.PublishRequest) error {
	//...
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(req.Metadata)
	if hasTTL {
		//... handle ttl for component.
	}
	//...
	return nil
}
```