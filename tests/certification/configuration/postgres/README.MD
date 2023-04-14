# Certification test for PostgreSQL configuration store

This project aims to test the Postgres Configuration Store component under various conditions.

## Init test
1. Initialize with non-existing table name. Verify the error
2. Initialize with upperCased table name. Verify the error
2. Initialize with correct table name. Should have no errors

## Test Get with multiple key versions
1. Add a key with empty version: returns added value
2. Add a key with version "1": returns value with version "1"
3. Add a key with version "2": returns value with version "2"
4. Add a key with non-numeric version: returns value with version "2"

## Test multiple channels to same table
1. Cerate multiple channels to same table
2. Start multiple applications and subscribe to all channels with each application for multiple keys
3. Verify all notifications received to all applications for every channel

## Network test
1. Simulate network interruption
2. Validate client receives updates for subscribed keys 

## Infra test:
1. Save a key-value pair in db
1. Stop postgres server and re-start
2. Validate client gets previous saved value using key