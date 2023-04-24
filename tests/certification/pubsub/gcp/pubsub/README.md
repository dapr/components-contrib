# GCP Pubsub Certification Test Plan
The purpose of this module is to provide tests that certify the GCP Pubsub as a stable component.

## Test Plan
### Certification Tests
- Verify with single publisher / single subscriber (GCPPubSubBasic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 2 topics
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
   - Verify that subscriber does not receive messages from the non-subscribed topic
- Verify data with an existing Topic (GCPPubSubExistingTopic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify with an optional parameter `disableEntityManagement` set to true (GCPPubSubEntityManagement)
   - Run dapr application with 1 publisher
   - Publisher tries to publish to 1 topic that is not present
   - Verify that the topic and subscriptions do not get created
   - Verify that the error is returned saying that the topic not present when publishing
- Verify data with an optional parameters `fifo` takes affect (GCPPubSubFIFOMessages)
   - Run dapr application with 2 publisher and 1 subscriber
   - Publishers publishe to 1 topic
   - Subscriber 1 subscribes to 1 topic
   - Message are expected to arrive in order
- Verify data with an optional parameters `deadLetterTopic` and `maxDeliveryAttempts` takes affect (GCPPubSubMessageDeadLetter)
   - Run dapr application with 1 publisher, 2 subscriber, and 1 topics
   - Publishers publishes to 1 topic
   - Subscriber 1 subscribes to 1 topic and fails causing messages to go to deadletter queue
   - Subscriber 2 polls messages from the deadletter queue
   - Message are expected to only be successfully consumed by Subscriber 2 from deadletter queue
### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

The runtime execution assumes that the appropriate GCP Authentication is established and the following environment variables are avialable:
- GCP_PROJECT
- GOOGLE_APPLICATION_CREDENTIALS
