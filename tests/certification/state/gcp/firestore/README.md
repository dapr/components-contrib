# GCP Firestore certification testing

This project aims to test the GCP Firestore State Store component under various conditions.

## Test plan

### Basic Test
1. Able to create and test connection.
2. Able to do set, fetch and delete.

### Test entity_kind
1. Able to create and test connection.
2. Able to specify Entity Kind to set, fetch and delete.

### Test NoIndex
1. Able to create and test connection.
2. Able to specify Entity Kind with NoIndex to set, fetch and delete.

## Run Tests
Note: 
**Currently, GCP Firestore in Datastore mode, does not provide a public GCP API.For setup, follow the instructions in the [GCP Firestore Documentation](https://cloud.google.com/datastore/docs/store-query-data).**

To run these tests, the environment variables `GCP_PROJECT_ID` and `GCP_FIRESTORE_ENTITY_KIND`
