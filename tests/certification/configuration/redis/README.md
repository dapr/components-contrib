# Redis Configuration Store certification testing

This project aims to test the Redis Configuration Store component under various conditions.

## Test plan

## Test subscribe with different events:
1. Subscribe to redis configuration store
2. Validate notifications received for all of the possible events: https://redis.io/docs/manual/keyspace-notifications/ (only "KA" config events)

## Infra test:
1. Stop redis server and re-start
2. Validate client gets previous saved keys
3. Validate client re-subscribes to previous subscribed keys

## Network test
1. Simulate network interruption
2. Validate client receives updates for subscribed keys 