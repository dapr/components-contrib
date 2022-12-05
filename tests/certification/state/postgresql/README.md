# PostgreSQL State Store certification testing

This project aims to test the PostgreSQL State Store component under various conditions.

To run these tests:

```sh
go test -v -tags certtests -count=1 .
```

## Test plan

## Initialization and migrations

Also test the `tableName` and `metadataTableName` metadata properties.

1. Initializes the component with names for tables that don't exist
1. Initializes the component with names for tables that don't exist, specifying an explicit schema
1. Initializes the component with all migrations performed (current level is "2")
1. Initializes the component with only the state table, created before the metadata table was added (implied migration level "1")

## Test for CRUD operations

1. Able to create and test connection.
2. Able to do set, get, update and delete.
3. Negative test to fetch record with key, that is not present.

## SQL Injection

* Not prone to SQL injection on write
* Not prone to SQL injection on read
* Not prone to SQL injection on delete

## Connection Recovery

1. When PostgreSQL goes down and then comes back up - client is able to connect

## Concurrency

1. Insert a Key-Value pair, eTag is set.
2. Update Value v2 for this Key with current eTag - eTag is updated.
3. Try to Update v3 for this Key with wrong eTag - value should not get updated.
4. Get and validate eTag, which should not have changed.

## Transactions

Upsert in Multi function, using 3 keys with updating values and TTL for 2 of the keys, down in the order.
