# Certification test for PostgreSQL configuration store

This project aims to test the Postgres Configuration Store component under various conditions.

## Init test
1. Initialize with non-existing table name. Verify the error
2. Initialize with correct table name. Should have no errors

## Test multiple channels to same table
1. Cerate multiple channels to same table
2. Start multiple applications and subscribe to all channels with each application 
3. Verify correct notification received to all applications for every channel

## Network test
1. Simulate network interruption
2. Validate client receives updates for subscribed keys 

## Infra test:
1. Save a key-value pair in db
1. Stop postgres server and re-start
2. Validate client gets previous saved value using key