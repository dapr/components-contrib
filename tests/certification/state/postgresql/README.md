# PostgreSQL State Store certification testing

This project aims to test the PostgreSQL State Store component under various conditions.

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

1. When PostgreSQL goes down and then comes back up - client is able to connect

## Concurrency

1. Insert a Key-Value pair, eTag is set.
2. Update Value v2 for this Key with current eTag - eTag is updated.
3. Try to Update v3 for this Key with wrong eTag - value should not get updated.
4. Get and validate eTag, which should not have changed.

## Transactions

Upsert in Multi function, using 3 keys with updating values and TTL for 2 of the keys, down in the order.
