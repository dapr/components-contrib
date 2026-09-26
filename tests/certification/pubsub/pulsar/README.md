### Pulsar Pubsub Certification

The purpose of this module is to provide tests that certify the Pulsar Pubsub as a stable component.

The Pulsar cluster starts once per authentication mode and is shared by the test flows. Each flow uses its own topic names; schema flows use dedicated topics matching their component configuration.

**Certification Tests**
- Verify with single publisher / single subscriber
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 2 topics
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
   - Verify that subscriber does not receive messages from the non-subscribed topic
- Verify with single publisher / multiple subscribers with same consumerID
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with single publisher / multiple subscribers with different consumerIDs
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with multiple publishers / multiple subscribers with different consumerIDs
   - Run dapr application with 2 publishers and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify data with a topic that does not exist
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of topic
   - Send messages to the topic created
   - Verify that subscriber received all the messages
- Verify reconnection after the network interruptions
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate network interruptions and verify that the component retires on error
   - Verify that all expected messages were received
- Verify data with deliverAfter/deliverAt set to a 15s delay
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Verify that subscriber receives no messages in the first 5s
   - Wait for message delay to pass
   - Verify that all expected messages were received
- Verify data with persistent topics after pulsar restart
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 1 topic
   - Restart pulsar service
   - Subscriber is subscribed to 1 topic
   - Verify that all expected messages were received
- Verify ordering process with partitioned topic
   - Run dapr application with 1 publisher and 2 subscriber
   - Publisher publishes to 1 4-partitioned topic
   - Subscriber is subscribed to 1 topic
   - Verify that all expected messages were received in order
- Verify Avro schema encode/decode round trip
   - Run dapr application with 1 publisher and 1 subscriber configured with `.avroschema`
   - Publisher publishes JSON messages that are Avro-encoded on the publish path
   - Subscriber receives messages that are decoded from Avro binary back to JSON
   - Verify that all expected messages were received with correct data