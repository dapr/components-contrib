# SQLite State Store certification testing

This project aims to test the SQLite State Store component under various conditions.

To run these tests:

```sh
go test -v -tags certtests -count=1 .
```

## Test plan

## Basic tests (in-memory)

Uses an in-memory, temporary database.

1. Creates a new, in-memory database and initializes the tables.
2. Able to do set, get, update and delete.

## SQL Injection

1. Not prone to SQL injection on write
2. Not prone to SQL injection on read
3. Not prone to SQL injection on delete

## Read-only databases

1. Open an existing database in read-only mode
2. Attempt basic CRUD tests: get operations succeed; set, update, delete fail.
3. Stop the Dapr sidecar and verify that the database file was not modified on-disk.

> This also tests the ability to use connection strings that don't start with `file:`

## TTLs and cleanups

1. Correctly parse the `cleanupIntervalInSeconds` metadata property:
   - No value uses the default value (3600 seconds)
   - A positive value sets the interval to the given number of seconds
   - A zero or negative value disables the cleanup
2. The cleanup method deletes expired records and updates the metadata table with the last time it ran
3. The cleanup method doesn't run if the last iteration was less than `cleanupIntervalInSeconds` or if another process is doing the cleanup
