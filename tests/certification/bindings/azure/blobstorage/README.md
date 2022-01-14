# Azure Blobstorage Binding certification testing

This project aims to test the Azure Blobstorage binding component under various conditions.

## Test plan

### Authentication tests

* Authenticate with Azure Active Directory using Service Principal Client Secret
* Authenticate with Storage Account Key

### Functional tests

- Delete request:
  - Successful deletion
  - File does not exist
  - Snapshots only
  - Base blob with its snapshots

- Get request:
  - Successful Get Request
  - Item does not exist

- Create request:
  - No filename specified
  - Filename specified
  - Correct MD5 Hash Provided
  - Incorrect MD5 hash specified
  - Existing file name specified (overwrites content)
  - Creating a public blob (depending on container setting)
  - Verifies automatic base64 decode option

- List request:
  - include custom metadata
  - include soft-deleted blobs
  - include snapshots
  - specify max results and use marker to retrieve more results
  - filter results by specifying a prefix

### Running the tests

This must be run in the GitHub Actions Workflow configured for test infrastructure setup.

If you have access to an Azure subscription you can run this locally on Mac or Linux after running `setup-azure-conf-test.sh` in `.github/infrastructure/conformance/azure` and then sourcing the generated bash rc file.