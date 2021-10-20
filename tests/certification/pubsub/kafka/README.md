# Kafka certifcation testing

This project aims to test the Kafka Pub/Sub component under various conditions.

## Test plan

### Basic conformance tests

* Bring up a 3-node Kafka cluster
    * Configured to have 10+ partitions per topic
* Start 1 sidecar/application
    * Test: Send 1000+ unique messages with keys set
    * App: Simulate periodic errors
    * Component: Retries on error
    * App: Observes successful messages
    * Test: Confirms that all expected messages were received (in order)

### Kafka infra tests

* Start a constant flow of publishing and subscribing
    * Test: Keeps count of total sent/received
* Start a second sidecar/application using the same consumer group
    * Component: Should properly handle a consumer rebalance
* Stop 1 broker node so that 2 of 3 are active
    * The 2 applications should handle the server rebalance
* Stop another broker so that 1 of 3 are active (loss of quorum)
    * Test: Begins trying to connect & publish
    * Component: Begins trying to connect & re-subscribe
* Restart both brokers so that 3 of 3 are active
    * Test & Component: Reconnect
    * Count of total sent should equal total received
* Start multiple consumers with different consumer groups
    * Test: Publishes a specific amount of messages
    * Each consumer should receive all messages

### Data integrity tests

* Start a new sidecar/application
* Verify cloud events 
    * Publish various cloud events
    * App receives Kafka messages and verifies their binary encoding
* Verify raw events
    * Publish various raw events
    * App receives Kafka messages and verifies their binary encoding