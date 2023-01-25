# AWS SNS/SQS Pubsub Certification Test Plan
The purpose of this module is to provide tests that certify the AWS SNS/SQS Pubsub as a stable component.

## Test Plan
### Certification Tests
- Verify with single publisher / single subscriber (SNSSQSBasic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 2 topics
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
   - Verify that subscriber does not receive messages from the non-subscribed topic
- Verify with single publisher / multiple subscribers with same consumerID (SNSSQSMultipleSubsSameConsumerIDs)
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with single publisher / multiple subscribers with different consumerIDs (SNSSQSMultipleSubsDifferentConsumerIDs)
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with multiple publishers / multiple subscribers with different consumerIDs (SNSSQSMultiplePubSubsDifferentConsumerIDs)
   - Run dapr application with 2 publishers and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify data with an existing Queue and existing Topic (SNSSQSExistingQueue)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify data with an existing Queue with a topic that does not exist (SNSSQSExistingQueueNonexistingTopic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify data with a topic that does not exist (SNSSQSNonexistingTopic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify with an optional parameter `disableEntityManagement` set to true (SNSSQSEntityManagement)
   - Run dapr application with 1 publisher
   - Publisher tries to publish to 1 topic that is not present
   - Verify that the topic and subscriptions do not get created
   - Verify that the error is returned saying that the topic not present when publishing
- Verify data with an optional parameter `messageVisibilityTimeout` takes affect (SNSSQSMessageVisibilityTimeout)
   - Run dapr application with 1 publisher and 2 subscriber
   - Subscriber 1 subscribes to 1 topic
   - Publisher publishes to 1 topic
   - Subscriber 2 waits for Subscriber 1 to be notify before subscribing to 1 topic
   - Subscriber 1 reeives message,  notifies subscriber 2 and sumlates being busy for time shorter than messageVisibilityTimeout seconds
   - Subscriber 2 receives go ahead and subscribes to 1 topic
   - Subscriber 2 must not receive message
- Verify data with an optional parameters `fifo` and `fifoMessageGroupID` takes affect (SNSSQSFIFOMessages)
   - Run dapr application with 2 publisher and 1 subscriber
   - Publishers publishe to 1 topic
   - Subscriber 1 subscribes to 1 topic
   - Message are expected to arrive in order
- Verify data with an optional parameters `sqsDeadLettersQueueName`, `messageRetryLimit`, and `messageReceiveLimit` takes affect (SNSSQSMessageDeadLetter)
   - Run dapr application with 1 publisher, 2 subscriber, and 2 topics
   - Publishers publishes to 1 topic
   - Subscriber 1 subscribes to 1 topic and fails causing messages to go to deadletter queue
   - Subscriber 2 subscribes to 2 topic connected to deadletter queue
   - Message are expected to arrive from only from Subscriber 2
### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

The runtime execution assumes that the appropriate AWS Profile or AWS environment variables (**AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY**) are configured.

Also, the AWS IAM Permissions need to be configured as indicated by this document [`Create an SNS/SQS instance`](https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-aws-snssqs/#create-an-snssqs-instance)