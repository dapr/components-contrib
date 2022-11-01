# MySQL State Store certification testing

This project aims to test the MySQL State Store component under various conditions. Tests are run against both MySQL 8 and MariaDB 10.

## Test plan

## Test for CRUD operations

1. Able to create and test connection.
2. Able to do set, get, update and delete.
3. Negative test to fetch record with key, that is not present.

## SQL Injection

* Not prone to SQL injection on write
* Not prone to SQL injection on read
* Not prone to SQL injection on delete

## Connection Recovery

1. When MySQL/MariaDB goes down and comes back up, client is able to connect

## Concurrency

a. Insert a Key-Value pair, eTag is set.
b. Update Value v2 for this Key with current eTag - eTag is updated.
c. Try to Update v3 for this Key with wrong eTag - value should not get updated.
d. Get and validate eTag, which should not have changed.

## Transactions

1. Upsert in Multi function, using 3 keys with updating values and TTL for 2 of the keys, down in the order.

## Close component

1. Ensure the database connection is closed when the component is closed.

## Metadata options

1. Without `schemaName`, check that the default one is used
2. Without `tableName`, check that the default one is used
3. Instantiate a component with a custom `schemaName` and validate it's used
4. Instantiate a component with a custom `tableName` and validate it's used
