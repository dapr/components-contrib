# Azure Event Hubs Pubsub certification testing

This project aims to test the Azure Event Hubs pubsub component under various conditions.

## Test Plan

### Pubsub Certification Tests
  - Test partition key, in order processing single publisher/subscriber (done as part of conformance tests)
  - Test single publisher / single subscriber
    - Start an app with 1 publisher and 1 subscriber
    - The publisher publishes to 2 topics 
    - The subscriber is subscribed to 1 topic
    - Test: Sends 100+ unique messages
    - App: Simulates periodic errors
    - Component: Retries on error
    - App: Observes successful messages
    - Test: Confirms that all expected messages were received
    - Test: Confirms that subscriber does not receive messages from the non-subscribed topic
  - Test single publisher / multiple subscribers with same consumerID
    - Start an app with 1 publisher and 2 subscribers
    - The publisher publishes to 1 topic with 2 partitions (EventHub scalable consumer pattern)
    - The subscriber is subscribed to 1 topic
    - Test: Sends 100+ unique messages
    - App: Simulates periodic errors
    - Component: Retries on error
    - App: Observes successful messages
    - Test: Confirms that all expected messages were received
    - Test: Confirms that each subscriber receive messages from only one partition
  - Test single publisher / multiple subscribers with different consumerIDs
    - Start an app with 1 publisher and 2 subscribers
    - The publisher publishes to 1 topic with 2 partitions (EventHub scalable consumer pattern)
    - The subscriber is subscribed to 1 topic
    - Test: Sends 100+ unique messages
    - App: Simulates periodic errors
    - Component: Retries on error
    - App: Observes successful messages
    - Test: Confirms that all expected messages were received
    - Test: Confirms that each subscriber receives messages from both partitions
 - Test multiple publishers / multiple subscribers with different consumerIDs
    - Start an app with 2 publishers and 2 subscribers
    - The publisher publishes to 1 topic with 2 partitions (EventHub scalable consumer pattern)
    - The subscriber is subscribed to 1 topic
    - Test: Sends 100+ unique messages from each publisher
    - App: Simulates periodic errors
    - Component: Retries on error
    - App: Observes successful messages
    - Test: Confirms that all expected messages were received
    - Test: Confirms that each subscriber receives messages from both partitions
 - Test entity management
   - Start a publisher and subscriber with a topic that does not exist
   - Test: Confirm creation of topic/eventHub in given eventHub namespace
   - Test: Send 100+ unique messasges to the newly created eventHub topic
   - App: Observe successful messages
   - Test: Confirm that subscriber receives all messages
 - Test IOT Event Hub : [TODO]
   - Start an app with 1 publisher and 1 subscriber
   - The publisher publishes to 1 IOT EventHub with 1 partition 
   - The subscriber is subscribed to 1 topic
   - Test: Sends 100+ unique messages
   - App: Simulates periodic errors
   - Component: Retries on error
   - App: Observes successful messages
   - Test: Confirms that all expected messages were received
   - Test: Confirms that all expected system properties are set
### Authentication Tests 
  - Test connection string based authentication mechanism
    - Connection string scoped at namespace
    - Connection string scoped at specific eventhub
  - Test AAD Service Principal based authentication
    - Utilize a service principal with appropriate roles granted
### Network Tests
  - Simulate network interruptions [TODO : network interruptions during publish]
    - Test: Simulate network interruptions 
    - Component: Begins to reconnect and resubscribe

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.

One can even run the test in local by just updating metadata values related to azure resources in `components/*/eventhubs.yaml`.