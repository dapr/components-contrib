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
* Verify maxPriority attribute is regarded.
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
