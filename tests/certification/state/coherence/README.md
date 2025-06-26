# Coherence State Store certification testing

This project aims to test the [Coherence State Store] component under various conditions.

This state store [supports the following features][features]:
* CRUD
* TTL

# Test plan

## Basic Test for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, update and delete.
3. Negative test to fetch record with key, that is not present.

## Test save or update data with different TTL settings:
1. Provide a TTL of 5 second:
    1. Fetch this record just after saving, should exist
    2. Sleep for 6 seconds
    3. Try to fetch again after a gap of 2 seconds, record shouldn't be found

## Test Bulk Operations

1. Able to issue SaveBulkState
2. Able to issue GetBulkState
3. Able to issue DeleteBulkState

## Out of scope

1. Tests for [features not implemented by Coherence][features] are out of scope. This includes
    * Transactional
    * ETag
    * Actors
    * Query


# References:

* [Coherence State Component reference page][Coherence State Store]
* [List of state stores and their features][features]
* [Coherence Go Client API reference](https://pkg.go.dev/github.com/oracle/coherence-go-client/v2/coherence)
* [Coherence Community](https://coherence.community/)

[Coherence State Store]: https://docs.dapr.io/reference/components-reference/supported-state-stores/setup-coherence/
[features]: https://docs.dapr.io/reference/components-reference/supported-state-stores/