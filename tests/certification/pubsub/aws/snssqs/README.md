# AWS SNS/SQS Pubsub Certification Test Plan
The purpose of this module is to provide tests that certify the AWS SNS/SQS Pubsub as a stable component.

## Test Plan
### Certification Tests
- Verify with single publisher / single subscriber (TestSNSSQSBasic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Publisher publishes to 2 topics
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
   - Verify that subscriber does not receive messages from the non-subscribed topic
- Verify with single publisher / multiple subscribers with same consumerID (TestSNSSQSMultipleSubsSameConsumerIDs)
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with single publisher / multiple subscribers with different consumerIDs (TestSNSSQSMultipleSubsDifferentConsumerIDs)
   - Run dapr application with 1 publisher and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify with multiple publishers / multiple subscribers with different consumerIDs (TestSNSSQSMultiplePubSubsDifferentConsumerIDs)
   - Run dapr application with 2 publishers and 2 subscribers
   - Publisher publishes to 1 topic
   - Subscriber is subscribed to 1 topic
   - Simulate periodic errors and verify that the component retires on error
   - Verify that all expected messages were received
- Verify data with an existing Queue and existing Topic (TestSNSSQSExistingQueue)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify data with an existing Queue with a topic that does not exist (TestSNSSQSExistingQueueNonexistingTopic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify data with a topic that does not exist (TestSNSSQSNonexistingTopic)
   - Run dapr application with 1 publisher and 1 subscriber
   - Verify the creation of service bus
   - Send messages to the service created
   - Verify that subscriber received all the messages
- Verify with an optional parameter `disableEntityManagement` set to true (TestSNSSQSEntityManagement)
   - Run dapr application with 1 publisher
   - Publisher tries to publish to 1 topic that is not present
   - Verify that the topic and subscriptions do not get created
   - Verify that the error is returned saying that the topic not present when publishing
- Verify data with an optional parameter `defaultMessageTimeToLiveInSec` set (TestSNSSQSDefaultTtl)
   - Run dapr application with 1 publisher and 1 subscriber
   - Subscriber is subscribed to 1 topic
   - Publisher publishes to 1 topic, wait double the TTL seconds
   - Verify the message is deleted/expired

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

The runtime execution assumes that the appropriate AWS Profile or AWS environment variables (**AWS_ACCESS_KEY_ID**, **AWS_SECRET_ACCESS_KEY**) are configured.

Also, the AWS IAM Permissions need to be configured as indicated by this document [`Create an SNS/SQS instance`](https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-aws-snssqs/#create-an-snssqs-instance)