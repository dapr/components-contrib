# Redis Configuration Store certification testing

This project aims to test the Redis Configuration Store component under various conditions.

## Test plan

## Test subscribe with different events:
1. Subscribe to redis configuration store
2. Validate notifications received for different redis events: https://redis.io/docs/manual/keyspace-notifications/ (only "Kg$xe" config events i.e. generic and string keyspace events)

## Network test
1. Simulate network interruption
2. Validate client receives updates for subscribed keys 
## Infra test:
1. Save a key-value pair in redis
1. Stop redis server and re-start
2. Validate client gets previous saved value using key