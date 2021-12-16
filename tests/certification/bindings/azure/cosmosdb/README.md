# Azure CosmosDB Binding certification testing

This project aims to test the Azure CosmosDB binding component under various conditions.

## Test plan

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Client Secret
* Authenticate with Master Key

### Other tests
- Verify data sent to output binding is written to Cosmos DB
- Expected failure for invalid partition key specified (Component Metadata Partition Key does not match Cosmos DB container)
- Expected failure for partition key missing from document
- Expected failure for `id` missing from document

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.