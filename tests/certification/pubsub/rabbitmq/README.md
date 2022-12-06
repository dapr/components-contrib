# RabbitMQ certifcation testing

This project aims to test the RabbitMQ Pub/Sub component under various conditions.

## Test plan

* Bring up a RabbitMQ cluster
* Test single publisher / single subscriber
    * Start an app with one publisher and one subscriber
    * The publisher advertises to 3 topic
    * The subscriber is subscribed to 2 topics
    * Test: Sends 1000+ unique messages with keys set
    * App: Simulates periodic errors
    * Component: Retries on error
    * App: Observes successful messages
    * Test: Confirms that all expected messages were received
    * Test: Confirms that subscriber does not receive messages from the non-subscribed topic
* Test single publisher / multiple subscribers with same consumer ID
    * Start one publisher and one subscriber with consumer ID "A"
    * Verify equality between sent and received messages
    * Start second subscriber with consumer ID "A"
    * Verify that *total number* of received messages by *both subscribers* equals to the number of successfully published messages
* Test single publisher / multiple subscribers with distinct consumer IDs
    * Start one publisher, one subscriber with consumer ID "A", and two subscribers with consumer ID "B"
    * Verify that the number of published messages equals to the sum of:
        * the number of messages received by subscriber "A"
        * the total number of the messages received by subscribers "B"
    * App: Simulates periodic errors
    * Component: Retries on error
* Test TTL is regarded.
  * Setting a large TTL only at the message level but not component level, wait for a small period, and verify that the message is received.
  * Setting a TTL only at the message level but not component level expires messages correctly
  * Setting a default TTL at the component level expires messages correctly
    * Create component spec with the field `ttlInSeconds`.
    * Run dapr application with component.
    * Send a message, wait TTL seconds, and verify the message is deleted/expired.
  * Setting a TTL at the component level and message level ignores the default component level TTL and always uses the message level TTL specified
