# Pub Sub

Pub Sub components provide a common way to interact with different message bus implementations to achieve reliable, high-scale scenarios based on event-driven async communications, while allowing users to opt-in to advanced capabilities using defined metadata.

## Implementing a new Pub Sub

A compliant pub sub needs to implement the `PubSub` inteface included in the [`pubsub.go`](pubsub.go) file.

### Message TTL (or Time To Live)

Message Time to live is implemented by default in Dapr. A publishing application can set the expiration of individual messages by publishing it with the `ttlInSeconds` metadata. Components that support message TTL should parse this metadata attribute. For components that do not implement this feature in Dapr, the runtime will automatically populate the `expiration` attribute in the CloudEvent object if `ttlInSeconds` is present - in this case, Dapr will expire the message when a Dapr subscriber is about to consume an expired message. The `expiration` attribute is handled by Dapr runtime as a convenience to subscribers, dropping expired messages without invoking subscribers' endpoint. Subscriber applications that don't use Dapr, need to handle this attribute and implement the expiration logic.

If the pub sub component implementation can handle message TTL natively without relying on Dapr, consume the `ttlInSeconds` metadata in the component implementation for the Publish function. Also, implement the `Features()` function so the Dapr runtime knows that it should not add the `expiration` attribute to events.

Example:

```go
import contribMetadata "github.com/dapr/components-contrib/metadata"

//...

func (c *MyComponent) Publish(req *pubsub.PublishRequest) error {
	//...
	ttl, hasTTL, _ := contribMetadata.TryGetTTL(req.Metadata)
	if hasTTL {
		//... handle ttl for component.
	}
	//...
	return nil
}

func (c *MyComponent) Features() []pubsub.Feature {
	// Tip: cache this list into a private property.
	// Simply return nil if component does not implement any addition features.
	return []pubsub.Feature{pubsub.FeatureMessageTTL}
}
```

For pub sub components that support TTL per topic or queue but not per message, there are some design choices:

 * Configure the TTL for the topic or queue as usual. Optionally, implement topic or queue provisioning in the Init() method, using the component configuration's metadata to determine the topic or queue TTL.
 * Let Dapr runtime handle `ttlInSeconds` for messages that want to expire earlier than the topic's or queue's TTL. So, applications can still benefit from TTL per message via Dapr for this scenario.

> Note: as per the CloudEvent spec, timestamps (like `expiration`) are formatted using RFC3339.
