# CockroachDB State Store Certification

The purpose of this module is to provide tests that certify the CockroachDB State Store as a stable component.

## Test plan

* Verify the queue is created/present.
    * Create component spec.
    * Run dapr application with component.
    * Ensure the queue is created/present.
* Verify the connection is established to RabbitMQ.
    * Create component spec.
    * Run dapr application with component.
    * Ensure that you have access to the queue and connection to the queue is established.