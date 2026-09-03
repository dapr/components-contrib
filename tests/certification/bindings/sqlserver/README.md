# SQL Server Output Binding Certification

The purpose of this module is to provide tests that certify the SQL Server Output Binding as a stable component.

## Test plan

* Verify the SQL Server binding is created/present
    * Create component spec
    * Run dapr application with component
    * Ensure the SQL Server connection is established
* Verify data is getting stored in SQL Server via the `exec` operation.
    * Create component spec with the data to be stored.
    * Run dapr application with component to store data in SQL Server as output binding.
    * Read stored data back with the `query` operation.
    * Ensure that read data is same as the data that was stored.
    * Verify the ability to use named parameters (`@p1`, `@p2`, ...) in both `exec` and `query`.
* Verify the `close` operation explicitly closes the DB connection, and that a subsequent operation fails.
* Verify reconnection to SQL Server for output binding.
    * Simulate a network error after sending some messages.
    * After the reconnection, send messages to SQL Server.
    * Ensure that the messages sent after the reconnection are stored in SQL Server.
