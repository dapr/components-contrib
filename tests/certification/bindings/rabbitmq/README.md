# RabbitMQ Binding Certification

The purpose of this module is to provide tests that certify the RabbitMQ Binding as a stable component.

## Test plan

* Verify the queue is created/present.
    * Create component spec.
    * Run dapr application with component.
    * Ensure the queue is created/present.
* Verify the connection is established to RabbitMQ.
    * Create component spec.
    * Run dapr application with component.
    * Ensure that you have access to the queue and connection to the queue is established.
* Verify data is getting stored in the queue.
    * Create component spec with the data to be stored.
    * Run dapr application with component to store data in the queue as output binding.
    * Read stored data from the queue as input binding.
    * Ensure that read data is same as the data that was stored.
* Verify Data level TTL is regarded.
    * Create component spec with the field `ttlInSeconds`.
    * Run dapr application with component.
    * Send a message, wait TTL seconds, and verify the message is deleted/expired.
* Verify durable attribute is regarded.
    * Create component spec with the field `durable` set true.
    * Run dapr application with component.
    * Send a message to the queue.
    * Ensure that the message is stored in the storage.
* Verify deleteWhenUnused attribute is regarded.
    * Create component spec with the field `deleteWhenUnused` set true.
    * Run dapr application with component.
    * Send a message to the queue.
    * Ensure that the message is deleted.
* Verify maxPriority attribute is regarded.
    * Create component spec with the field `maxPriority`.
    * Run dapr application with component.
    * Ensure that the priority queue is created.
* Verify exclusive attribute is regarded.
    * Create component spec with the field `exclusive`.
    * Run dapr application with component.
    * Ensure that the topic is exclusive.
* Verify reconnection to the queue for output binding.
    * Simulate a network error before sending any messages.
    * Run dapr application with the component.
    * After the reconnection, send messages to the queue.
    * Ensure that the messages sent after the reconnection are sent to the queue.
* Verify reconnection to the queue for input binding.
    * Simulate a network error before reading any messages.
    * Run dapr application with the component.
    * After the reconnection, read messages from the queue.
    * Ensure that the messages after the reconnection are read.

## Queue TTL regression checks

The binding package also has focused integration tests for queue TTL declaration.
With `DAPR_TEST_RABBITMQ_HOST` set to an isolated RabbitMQ broker's connection
string, run from the repository root:

```sh
go test -tags=integration_test ./bindings/rabbitmq \
  -run '^(TestQueueTTLDeclaration|TestQueuesWithTTL|TestPublishingWithTTL)$'
```

These checks cover new and existing queues with no TTL, a one-second TTL, values
around the signed 32-bit millisecond boundary, and a 30-day TTL
(`ttlInSeconds: 2592000` or `ttl: 720h`). Queue arguments must retain the exact
millisecond value using AMQP's 64-bit integer encoding. Redeclaring a queue
created with a 32-bit TTL must remain compatible, including its short-message
expiry behavior. Long TTLs are checked by broker-enforced argument equivalence,
not by waiting for messages to expire.
