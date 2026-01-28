# MySQL Output Binding Certification

The purpose of this module is to provide tests that certify the MySQL Output Binding as a stable component.

## Test plan

* Verify the mysql is created/present
    * Create component spec
    * Run dapr application with component
    * Ensure the mysql is present
* Verify the connection is established to mysql.
    * Create component spec.
    * Run dapr application with component.
    * Ensure that you have access to mysql and connection to mysql DB is established.
* Verify data is getting stored in mysql DB.
    * Create component spec with the data to be stored.
    * Run dapr application with component to store data in mysql as output binding.
    * Read stored data from mysql.
    * Ensure that read data is same as the data that was stored.
    * Verify the ability to use named parameters in queries.
* Verify reconnection to mysql for output binding.
    * Simulate a network error before sending any messages.
    * Run dapr application with the component.
    * After the reconnection, send messages to mysql.
    * Ensure that the messages sent after the reconnection are stored in mysql.
