# PostgreSQL Output Binding Certification

The purpose of this module is to provide tests that certify the PostgreSQL Output Binding as a stable component.

## Test plan

* Verify the postgres is created/present
    * Create component spec
    * Run dapr application with component
    * Ensure the postgres is present
* Verify the connection is established to postgres.
    * Create component spec.
    * Run dapr application with component.
    * Ensure that you have access to postgres and connection to postgres DB is established.
* Verify data is getting stored in postgres DB.
    * Create component spec with the data to be stored.
    * Run dapr application with component to store data in postgres as output binding.
    * Read stored data from postgres.
    * Ensure that read data is same as the data that was stored.
* Verify reconnection to postgres for output binding.
    * Simulate a network error before sending any messages.
    * Run dapr application with the component.
    * After the reconnection, send messages to postgres.
    * Ensure that the messages sent after the reconnection are stored in postgres.