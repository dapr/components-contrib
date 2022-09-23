# MongoDB State Store certification testing

This project aims to test the MongoDB State Store component under various conditions.

## Test plan

## Basic Test for CRUD operations:
1. Able to create and test connection.
2. Able to do set, fetch, update and delete.
3. Negative test to fetch record with key, that is not present.

## Component must reconnect when server or network errors are encountered

## Infra test:
1. When MongoDB goes down and then comes back up - client is able to connect
