# Kafka certifcation testing

This project aims to test the Kafka Pub/Sub component under various conditions.

## Test plan

### Basic tests

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
* Start a second sidecar/application using a different consumer group
    * Test: Publishes a specific amount of messages
    * Each consumer group should receive all messages
* Start third consumer with a matching consumer group
    * Test: Publishes a specific amount of messages
    * Component: Between the each of the consumers in the group, all messages should be consumed, but not necessarily in order.
* Stop 1 broker node so that 2 of 3 are active
    * The 2 applications should handle the server rebalance
* Stop another broker so that 1 of 3 are active (loss of quorum)
    * Test: Begins trying to reconnect & publish
    * Component: Begins trying to reconnect & re-subscribe
* Stop the last broker so that 0 of 3 are active (complete outage)
    * Same as reconnection behavior above
* Restart both brokers so that 3 of 3 are active
    * Test & Component: Reconnect
    * Count of total sent should equal total received
* Stop consumer with >1 sidecar subscribed
    * Test: Publishes messages in the background
    * Component: Handles a consumer rebalance

### Network tests

* Simulate network interruption
    * Test: Begins trying to reconnect & publish
    * Component: Begins trying to reconnect & re-subscribe

### Data integrity tests

* **TODO** Start a new sidecar/application
* **TODO** Verify cloud events 
    * **TODO** Publish various cloud events
    * **TODO** App receives Kafka messages and verifies their binary encoding
* **TODO** Verify raw events
    * **TODO** Publish various raw events
    * **TODO** App receives Kafka messages and verifies their binary encoding
