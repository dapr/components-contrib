# Azure Blob storage certification testing

This project aims to test the Azure Blob storage State Store component under various conditions.

## Test plan

## Existing storage container with access key authentication:
1. Able to create and test connection.
2. Able to do set, fetch, delete.

## Non-existing container is created - using access key authentication:
1. Able to create and test connection.
2. Able to do set, fetch, delete data.
3. Delete the container (cleanup step)

## Authentication using Azure Active Directory Auth
1. Save Data and retrieve data using AAD credentials
