# MQTT certifcation testing

This project aims to test the MQTT Pub/Sub component under various conditions.

## Test plan

### Basic Test

* Bring up a MQTT cluster
* Start 1 sidecar/application(App1)
    * Publishes 1000+ unique messages 
    * App: Simulate periodic errors
    * Component: Retries on error
    * App: Observes successful messages
    * Test: Confirms that all expected messages were received

### Multiple Publishers-Subscribers

* Start second sidecar/application(App2)
    * Each of the publishers publish a fixed number of messages to the topic
    * Test: Confirms that both applications receive all published messages

### Infra Test

* Start a constant flow of publishing and subscribing(App1)
    * Test: Keeps count of total sent/received
* Start another sidecar/application with persistent session(App2)
    * Test: Publishes messages in background
    * Each of the applications should receive messages
* Stop consumer connected with persistent session(App2)
    * Test: Publishes messages in background
    * Only App1 should receive messages
* Stop publisher as well so that none of the components are active
    * No messages are published and received
* Restart second consumer with persistent session
    * App2 receives all lost messages
* Restart publisher so that both components are active
    * Test: Confirms that both applications received all published messages and no messages were lost

### Network Test
* Simulate network interruption
    * Test: Begins trying to reconnect & publish
    * Component: Begins trying to reconnect & re-subscribe