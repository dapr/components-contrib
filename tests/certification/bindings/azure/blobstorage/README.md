# Azure Blobstorage Binding certification testing

This project aims to test the Azure Blobstorage binding component under various conditions.

## Test plan

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Client Secret
* Authenticate with Storage Account Key

### Other tests
- TODO: Get
  - Successful Get
  - Item does not exist
- TODO: List
  - List
  - Various options...
- TODO: Delete
  - Successful delete
  - File does not exist
- TODO: Create
  - No filename specified
  - Filename specified
  - MD5 Hash Provided
  - Incorrect MD5 Hash Provided
  - Incorrect Content Type provided
  - File already exists

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.