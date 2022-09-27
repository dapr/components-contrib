# Memcached State Store certification testing

This project aims to test the [Memcached State Store] component under various conditions.

This state store [supports the following features][features]:
* CRUD
* TTL

# Test plan

## Basic Test for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, update and delete.
3. Negative test to fetch record with key, that is not present.

## Test save or update data with different TTL settings:
1. TTL not expiring ([`0` for memcached](https://github.com/memcached/memcached/wiki/Commands#set))
2. TTL not a valid number
3. Provide a TTL of 1 second:
    1. Fetch this record just after saving
    2. Sleep for 2 seconds
    3. Try to fetch again after a gap of 2 seconds, record shouldn't be found


## Test network instability
1. Configure memcache with a known (non-default) timeout of 20 seconds.
2. Set a key to show the connection is fine. Make the TTL is way bigger than the timeout. Say 4x its value.
3. Interrupt the network (the memcache ports) for longer than the established timeout value.
4. Wait a few seconds seconds (less than the timeout value).
5. Try to read the key written on step 2 and assert its.

## Out of scope

1. Tests verifying content persistence on Memcached reloads are out of scope as Memcached data is ephemeral.
2. Tests for [features not implemented by Memcached][features] are out of scope. This includes
    * Transactional
    * ETag
        + Notice that memcached has the concept of [64-bit CheckAndSet (CAS) values][cas] but that doesn't translate cleanly to ETags.
    * Actors
    * Query


# References:

* [Memcache State Component reference page][Memcached State Store]
* [List of state stores and their features][features]
* [Memcached API reference](https://github.com/memcached/memcached)
* [gomemcache - our client documentation](https://pkg.go.dev/github.com/bradfitz/gomemcache/memcache)

[Memcached State Store]: https://docs.dapr.io/reference/components-reference/supported-state-stores/setup-memcached/
[features]: https://docs.dapr.io/reference/components-reference/supported-state-stores/
[cas]: https://github.com/memcached/memcached/wiki/Commands#cas