# SQLite State Store certification testing

This project aims to test the SQLite State Store component under various conditions.

To run these tests:

```sh
go test -v -tags certtests -count=1 .
```

## Test plan

## Initialization and migrations

Also test the `tableName` and `metadataTableName` metadata properties.

1. Initializes the component with names for tables that don't exist
2. Initializes the component with names for tables that don't exist, specifying an explicit schema
3. Initializes the component with all migrations performed (current level is "2")
4. Initializes the component with only the state table, created before the metadata table was added (implied migration level "1")
5. Initializes three components at the same time and ensure no race conditions exist in performing migrations

## Test for CRUD operations

1. Able to create and test connection.
2. Able to do set, get, update and delete.
3. Negative test to fetch record with key, that is not present.

## SQL Injection

* Not prone to SQL injection on write
* Not prone to SQL injection on read
* Not prone to SQL injection on delete

## TTLs and cleanups

1. Correctly parse the `cleanupIntervalInSeconds` metadata property:
   - No value uses the default value (3600 seconds)
   - A positive value sets the interval to the given number of seconds
   - A zero or negative value disables the cleanup
2. The cleanup method deletes expired records and updates the metadata table with the last time it ran
3. The cleanup method doesn't run if the last iteration was less than `cleanupIntervalInSeconds` or if another process is doing the cleanup

## Connection Recovery

1. When PostgreSQL goes down and then comes back up - client is able to connect

## Concurrency

1. Insert a Key-Value pair, eTag is set.
2. Update Value v2 for this Key with current eTag - eTag is updated.
3. Try to Update v3 for this Key with wrong eTag - value should not get updated.
4. Get and validate eTag, which should not have changed.

## Transactions

Upsert in Multi function, using 3 keys with updating values and TTL for 2 of the keys, down in the order.
