# RabbitMQ Binding Certification

The purpose of this module is to provide tests that certify the Redis Binding as a stable component.

## Test plan

* Verify that data is getting stored in Redis.
    * Create the component spec.
    * Run dapr application to store data in Redis component as output binding.
    * Ensure that connection to redis is established.
    * Read stored data from Redis and verify that the data inserted is present.
* Verify that data is successfully retrieved during a network interruption.
    * Create the component spec.
    * Run dapr application to store data in Redis component as output binding.
    * Insert data in the Redis component as output binding before network interruption.
    * Check if the data is accessible after the network is up again.
    * Restart the Redis server.
    * Check if the data is accessible after the server is up again.
* Verify that the client is able to successfully retry during connection issues.
    * Create the component spec.
    * Run dapr application to store data in Redis component as output binding.
    * Restart the Redis server.
    * Insert data into the Redis store as output binding during the server restart.
    * Check if the data is accessible after the server is up again.
