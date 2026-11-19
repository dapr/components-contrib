# Redis Pub/Sub certification testing

This project aims to test the Redis Pub/Sub component under various conditions.

## Test plan

* Subscriptions survive repeated app health flaps
    * Start Redis 7 and an app subscribed to two topics that receive no messages, with app health checks enabled and `poolSize: 6`
    * Flap app health four times, so the runtime stops and restarts the subscriptions each time
    * Test: Only one `XREADGROUP` connection per topic is left blocked on the server
    * Test: Messages published to both topics after the flaps are delivered
