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
