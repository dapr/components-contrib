# Azure Cosmos DB certification testing

This project aims to test the Azure Cosmos DB State Store component under various conditions.

## Test plan

### Basic Test using master key authentication
1. Able to create and test connection.
2. Able to do set, fetch and delete.

### Test Partition Keys
Ensure the following scenarios:
 1. In case of invalid partition key, the access fails
 2. In case of missing partition key, *key* property is used in place of it
 3. In case of correct partition key, the access succeeds

### Authentication using Azure AD
1. Save Data and retrieve data using AAD credentials